package tools

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"pentagentx/pkg/database"
	"pentagentx/pkg/docker"
	obs "pentagentx/pkg/observability"
	"pentagentx/pkg/observability/langfuse"

	"github.com/docker/docker/api/types/container"
	"github.com/sirupsen/logrus"
)

const (
	defaultExecCommandTimeout = 5 * time.Minute
	defaultExtraExecTimeout   = 5 * time.Second
	defaultQuickCheckTimeout  = 500 * time.Millisecond
	timeoutCleanupTimeout     = 5 * time.Second

	// ANSI terminal color codes (aligned with PentAgentX UI palette)
	ansiColorInputCmd  = "\033[96m" // Bright Cyan - matches UI blue accents
	ansiColorSystemMsg = "\033[92m" // Bright Green - universal success/info
	ansiColorReset     = "\033[0m"  // Reset to default
	ansiLineTerminator = "\r\n"     // CRLF for terminal compatibility
)

type execResult struct {
	output string
	err    error
}

type terminal struct {
	flowID       int64
	taskID       *int64
	subtaskID    *int64
	containerID  int64
	containerLID string
	dockerClient docker.DockerClient
	tlp          TermLogProvider
}

func NewTerminalTool(
	flowID int64,
	taskID, subtaskID *int64,
	containerID int64, containerLID string,
	dockerClient docker.DockerClient,
	tlp TermLogProvider,
) Tool {
	return &terminal{
		flowID:       flowID,
		taskID:       taskID,
		subtaskID:    subtaskID,
		containerID:  containerID,
		containerLID: containerLID,
		dockerClient: dockerClient,
		tlp:          tlp,
	}
}

func (t *terminal) wrapCommandResult(ctx context.Context, args json.RawMessage, name, result string, err error) (string, error) {
	ctx, observation := obs.Observer.NewObservation(ctx)
	if err != nil {
		observation.Event(
			langfuse.WithEventName("terminal tool error swallowed"),
			langfuse.WithEventInput(args),
			langfuse.WithEventStatus(err.Error()),
			langfuse.WithEventLevel(langfuse.ObservationLevelWarning),
			langfuse.WithEventMetadata(langfuse.Metadata{
				"tool_name": name,
				"error":     err.Error(),
			}),
		)

		logrus.WithContext(ctx).WithError(err).WithFields(logrus.Fields{
			"tool":   name,
			"result": result[:min(len(result), 1000)],
		}).Error("terminal tool failed")
		return fmt.Sprintf("terminal tool '%s' handled with error: %v", name, err), nil
	}
	return result, nil
}

func (t *terminal) Handle(ctx context.Context, name string, args json.RawMessage) (string, error) {
	if !t.IsAvailable() {
		return "", fmt.Errorf("terminal is not available")
	}

	logger := logrus.WithContext(ctx).WithFields(enrichLogrusFields(t.flowID, t.taskID, t.subtaskID, logrus.Fields{
		"tool": name,
		"args": string(args),
	}))

	switch name {
	case TerminalToolName:
		var action TerminalAction
		if err := json.Unmarshal(args, &action); err != nil {
			logger.WithError(err).Error("failed to unmarshal terminal action")
			return "", fmt.Errorf("failed to unmarshal terminal action: %w", err)
		}
		timeout := time.Duration(action.Timeout)*time.Second + defaultExtraExecTimeout
		result, err := t.ExecCommand(ctx, action.Cwd, action.Input, action.Detach.Bool(), timeout)
		return t.wrapCommandResult(ctx, args, name, result, err)
	case FileToolName:
		var action FileAction
		if err := json.Unmarshal(args, &action); err != nil {
			logger.WithError(err).Error("failed to unmarshal file action")
			return "", fmt.Errorf("failed to unmarshal file action: %w", err)
		}

		logger = logger.WithFields(logrus.Fields{
			"action": action.Action,
			"path":   action.Path,
		})

		switch action.Action {
		case ReadFile:
			result, err := t.ReadFile(ctx, t.flowID, action.Path)
			return t.wrapCommandResult(ctx, args, name, result, err)
		case UpdateFile:
			result, err := t.WriteFile(ctx, t.flowID, action.Content, action.Path)
			return t.wrapCommandResult(ctx, args, name, result, err)
		default:
			logger.Error("unknown file action")
			return "", fmt.Errorf("unknown file action: %q (valid actions: read_file, update_file). To create a new file, use action \"update_file\"", action.Action)
		}
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

func (t *terminal) ExecCommand(
	ctx context.Context,
	cwd, command string,
	detach bool,
	timeout time.Duration,
) (string, error) {
	containerName := PrimaryTerminalName(t.flowID)

	if err := rejectInteractiveTerminalCommand(command); err != nil {
		return "", err
	}

	// create options for starting the exec process
	cmd := []string{
		"sh",
		"-c",
		command,
	}

	// verify container runtime status
	isRunning, err := t.dockerClient.IsContainerRunning(ctx, t.containerLID)
	if err != nil {
		return "", fmt.Errorf("runtime verification failed: %w", err)
	}
	if !isRunning {
		return "", fmt.Errorf("container runtime is not operational")
	}

	if cwd == "" {
		cwd = docker.WorkFolderPathInContainer
	}

	// Format command with working directory and ANSI styling
	styledCommand := fmt.Sprintf("%s $ %s%s%s%s", cwd, ansiColorInputCmd, command, ansiColorReset, ansiLineTerminator)
	_, err = t.tlp.PutMsg(ctx, database.TermlogTypeStdin, styledCommand, t.containerID, t.taskID, t.subtaskID)
	if err != nil {
		return "", fmt.Errorf("failed to put terminal log (stdin): %w", err)
	}

	if timeout <= 0 || timeout > 20*time.Minute {
		timeout = defaultExecCommandTimeout
	}

	createResp, err := t.dockerClient.ContainerExecCreate(ctx, containerName, container.ExecOptions{
		Cmd:          cmd,
		Env:          nonInteractiveTerminalEnv(),
		AttachStdout: true,
		AttachStderr: true,
		WorkingDir:   cwd,
		Tty:          true,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create exec process: %w", err)
	}

	if detach {
		resultChan := make(chan execResult, 1)
		detachedCtx := context.WithoutCancel(ctx)

		go func() {
			output, err := t.getExecResult(detachedCtx, createResp.ID, command, timeout)
			resultChan <- execResult{output: output, err: err}
		}()

		select {
		case result := <-resultChan:
			if result.err != nil {
				return "", fmt.Errorf("command failed: %w: %s", result.err, result.output)
			}
			if result.output == "" {
				return "Command completed in background with exit code 0", nil
			}
			return result.output, nil
		case <-time.After(defaultQuickCheckTimeout):
			return fmt.Sprintf("Command started in background with timeout %s (still running)", timeout), nil
		}
	}

	return t.getExecResult(ctx, createResp.ID, command, timeout)
}

func (t *terminal) getExecResult(ctx context.Context, id, command string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// attach to the exec process
	resp, err := t.dockerClient.ContainerExecAttach(ctx, id, container.ExecAttachOptions{
		Tty: true,
	})
	if err != nil {
		return "", fmt.Errorf("failed to attach to exec process: %w", err)
	}
	defer resp.Close()

	dst := bytes.Buffer{}
	errChan := make(chan error, 1)

	go func() {
		_, copyErr := io.Copy(&dst, resp.Reader)
		errChan <- copyErr
	}()

	select {
	case err := <-errChan:
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("failed to copy output: %w", err)
		}
	case <-ctx.Done():
		// Close the response to unblock io.Copy
		resp.Close()

		// Wait for the copy goroutine to finish
		<-errChan

		if strings.TrimSpace(command) != "" {
			if cleanupErr := t.cleanupTimedOutCommand(context.WithoutCancel(ctx), id, command); cleanupErr != nil {
				logrus.WithContext(ctx).WithError(cleanupErr).WithFields(logrus.Fields{
					"exec_id": id,
					"tool":    TerminalToolName,
				}).Warn("failed to cleanup timed out terminal command")
			}
		}

		suggestedTimeout := max(int(timeout.Seconds())-10, 10)
		partialOutput := dst.String()
		if strings.TrimSpace(partialOutput) != "" {
			styledOutput := fmt.Sprintf("%s%s%s%s", ansiColorSystemMsg, partialOutput, ansiColorReset, ansiLineTerminator)
			if _, logErr := t.tlp.PutMsg(context.WithoutCancel(ctx), database.TermlogTypeStdout, styledOutput, t.containerID, t.taskID, t.subtaskID); logErr != nil {
				logrus.WithContext(ctx).WithError(logErr).Warn("failed to put partial terminal log after timeout")
			}
			return fmt.Sprintf(
				"Command timed out before clean exit, but produced partial output:\n%s\n\n"+
					"HINT: The command may be interactive or waiting for the remote side to close. "+
					"Use bounded non-interactive forms such as 'timeout %d <command>' or command-specific wait/quit flags.",
				truncateString(partialOutput, 4000),
				suggestedTimeout,
			), nil
		}
		return "", fmt.Errorf(
			"command execution timeout (%v). Partial output: %s. "+
				"HINT: If this is an interactive command (shell/REPL/listener), use detach=true. "+
				"For long batch commands, wrap with shell timeout utility: 'timeout %d <command>' to ensure clean completion",
			ctx.Err(),
			truncateString(dst.String(), 500),
			suggestedTimeout,
		)
	}

	// wait for the exec process to finish
	_, err = t.dockerClient.ContainerExecInspect(ctx, id)
	if err != nil {
		return "", fmt.Errorf("failed to inspect exec process: %w", err)
	}

	results := dst.String()
	// Style system output with color coding
	styledOutput := fmt.Sprintf("%s%s%s%s", ansiColorSystemMsg, results, ansiColorReset, ansiLineTerminator)
	_, err = t.tlp.PutMsg(ctx, database.TermlogTypeStdout, styledOutput, t.containerID, t.taskID, t.subtaskID)
	if err != nil {
		return "", fmt.Errorf("failed to put terminal log (stdout): %w", err)
	}

	if results == "" {
		results = "Command completed successfully with exit code 0. No output produced (silent success)"
	}

	return results, nil
}

func (t *terminal) cleanupTimedOutCommand(ctx context.Context, execID, command string) error {
	ctx, cancel := context.WithTimeout(ctx, timeoutCleanupTimeout)
	defer cancel()

	inspect, err := t.dockerClient.ContainerExecInspect(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to inspect timed out exec process: %w", err)
	}

	var cleanupParts []string
	if inspect.Pid > 0 {
		pid := strconv.Itoa(inspect.Pid)
		cleanupParts = append(cleanupParts,
			fmt.Sprintf("kill -TERM -%s 2>/dev/null || true", pid),
			fmt.Sprintf("pkill -TERM -P %s 2>/dev/null || true", pid),
			fmt.Sprintf("kill -TERM %s 2>/dev/null || true", pid),
		)
	}
	if trimmedCommand := strings.TrimSpace(command); trimmedCommand != "" {
		quotedCommand := shellSingleQuote(trimmedCommand)
		cleanupParts = append(cleanupParts,
			fmt.Sprintf("target=%s; self=$$; ps -eo pid=,args= | awk -v target=\"$target\" -v self=\"$self\" '$1 != self { line=$0; sub(/^[[:space:]]*[0-9]+[[:space:]]+/, \"\", line); if (index(line, target)>0) print $1 }' | xargs -r kill -TERM 2>/dev/null || true", quotedCommand),
		)
	}
	cleanupParts = append(cleanupParts, knownInteractiveCleanupCommands("TERM")...)

	if len(cleanupParts) == 0 {
		return nil
	}

	cleanupParts = append(cleanupParts, "sleep 1")
	if inspect.Pid > 0 {
		pid := strconv.Itoa(inspect.Pid)
		cleanupParts = append(cleanupParts,
			fmt.Sprintf("kill -KILL -%s 2>/dev/null || true", pid),
			fmt.Sprintf("pkill -KILL -P %s 2>/dev/null || true", pid),
			fmt.Sprintf("kill -KILL %s 2>/dev/null || true", pid),
		)
	}
	if trimmedCommand := strings.TrimSpace(command); trimmedCommand != "" {
		quotedCommand := shellSingleQuote(trimmedCommand)
		cleanupParts = append(cleanupParts,
			fmt.Sprintf("target=%s; self=$$; ps -eo pid=,args= | awk -v target=\"$target\" -v self=\"$self\" '$1 != self { line=$0; sub(/^[[:space:]]*[0-9]+[[:space:]]+/, \"\", line); if (index(line, target)>0) print $1 }' | xargs -r kill -KILL 2>/dev/null || true", quotedCommand),
		)
	}
	cleanupParts = append(cleanupParts, knownInteractiveCleanupCommands("KILL")...)

	cleanupCommand := strings.Join(cleanupParts, "; ")
	createResp, err := t.dockerClient.ContainerExecCreate(ctx, PrimaryTerminalName(t.flowID), container.ExecOptions{
		Cmd:          []string{"sh", "-c", cleanupCommand},
		AttachStdout: true,
		AttachStderr: true,
		Tty:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to create timeout cleanup exec process: %w", err)
	}

	_, err = t.getExecResult(ctx, createResp.ID, "", timeoutCleanupTimeout)
	if err != nil {
		return fmt.Errorf("failed to run timeout cleanup command: %w", err)
	}
	return nil
}

func (t *terminal) cleanupActiveCommands(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, timeoutCleanupTimeout)
	defer cancel()

	cleanupParts := knownInteractiveCleanupCommands("TERM")
	cleanupParts = append(cleanupParts, "sleep 1")
	cleanupParts = append(cleanupParts, knownInteractiveCleanupCommands("KILL")...)
	cleanupCommand := strings.Join(cleanupParts, "; ")

	createResp, err := t.dockerClient.ContainerExecCreate(ctx, PrimaryTerminalName(t.flowID), container.ExecOptions{
		Cmd:          []string{"sh", "-c", cleanupCommand},
		AttachStdout: true,
		AttachStderr: true,
		Tty:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to create terminal cleanup exec process: %w", err)
	}

	if _, err := t.getExecResult(ctx, createResp.ID, "", timeoutCleanupTimeout); err != nil {
		return fmt.Errorf("failed to run terminal cleanup command: %w", err)
	}
	return nil
}

func knownInteractiveCleanupCommands(signal string) []string {
	patterns := []string{
		"[m]ysql([[:space:]]|$)",
		"[p]sql([[:space:]]|$)",
		"[r]edis-cli([[:space:]]|$)",
		"[s]sh([[:space:]]|$)",
		"[t]elnet([[:space:]]|$)",
		"[f]tp([[:space:]]|$)",
		"[s]ftp([[:space:]]|$)",
	}

	commands := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		commands = append(commands, fmt.Sprintf(
			"pattern=%s; self=$$; ps -eo pid=,stat=,args= | awk -v pattern=\"$pattern\" -v self=\"$self\" '$1 != self && $2 !~ /Z/ { line=$0; sub(/^[[:space:]]*[0-9]+[[:space:]]+[^[:space:]]+[[:space:]]+/, \"\", line); if (line ~ pattern) print $1 }' | xargs -r kill -%s 2>/dev/null || true",
			shellSingleQuote(pattern),
			signal,
		))
	}
	return commands
}

func rejectInteractiveTerminalCommand(command string) error {
	tokens := splitShellFields(command)
	for i, token := range tokens {
		name := shellCommandName(token)
		if name == "" {
			continue
		}

		switch name {
		case "mysql":
			if hasInteractiveMysqlPasswordFlag(tokens[i+1:]) {
				return fmt.Errorf("interactive terminal command rejected: mysql '-p' or '--password' without an inline value waits for a password prompt; use MYSQL_PWD, '-p<password>', or '--password=<password>' with a non-interactive query")
			}
		case "psql":
			if !hasNonInteractiveSQLFlag(tokens[i+1:]) && !hasPipeBefore(tokens, i) {
				return fmt.Errorf("interactive terminal command rejected: psql without '-c' or '-f' opens an interactive session")
			}
		case "redis-cli":
			if !hasRedisCommand(tokens[i+1:]) {
				return fmt.Errorf("interactive terminal command rejected: redis-cli without a command opens an interactive session")
			}
		case "ssh", "telnet", "ftp", "sftp":
			return fmt.Errorf("interactive terminal command rejected: %s opens an interactive network session", name)
		}
	}
	return nil
}

func splitShellFields(command string) []string {
	fields := make([]string, 0)
	var builder strings.Builder
	var quote rune
	escaped := false

	flush := func() {
		if builder.Len() == 0 {
			return
		}
		fields = append(fields, builder.String())
		builder.Reset()
	}

	for _, r := range command {
		switch {
		case escaped:
			builder.WriteRune(r)
			escaped = false
		case r == '\\' && quote != '\'':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
				continue
			}
			builder.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		case r == '|' || r == ';':
			flush()
			fields = append(fields, string(r))
		default:
			builder.WriteRune(r)
		}
	}
	flush()
	return fields
}

func shellCommandName(token string) string {
	token = strings.TrimSpace(token)
	if token == "" || token == "|" || token == ";" || token == "&&" || token == "||" {
		return ""
	}
	if strings.Contains(token, "=") && !strings.Contains(token, "/") {
		return ""
	}
	token = strings.Trim(token, "\"'")
	token = filepath.Base(token)
	return strings.ToLower(token)
}

func hasInteractiveMysqlPasswordFlag(tokens []string) bool {
	for _, token := range tokens {
		if isCommandSeparator(token) {
			return false
		}
		lower := strings.ToLower(token)
		if token == "-p" || lower == "--password" || lower == "--password=" {
			return true
		}
	}
	return false
}

func hasNonInteractiveSQLFlag(tokens []string) bool {
	for _, token := range tokens {
		if isCommandSeparator(token) {
			return false
		}
		lower := strings.ToLower(token)
		if lower == "-c" || lower == "--command" || strings.HasPrefix(lower, "--command=") ||
			lower == "-f" || lower == "--file" || strings.HasPrefix(lower, "--file=") {
			return true
		}
	}
	return false
}

func hasRedisCommand(tokens []string) bool {
	skipNext := false
	for _, token := range tokens {
		if isCommandSeparator(token) {
			return false
		}
		if skipNext {
			skipNext = false
			continue
		}
		if strings.HasPrefix(token, "-") {
			skipNext = redisOptionNeedsValue(token)
			continue
		}
		return true
	}
	return false
}

func redisOptionNeedsValue(token string) bool {
	switch strings.ToLower(token) {
	case "-h", "-p", "-a", "-n", "-u", "--user", "--pass", "--raw":
		return true
	default:
		return false
	}
}

func hasPipeBefore(tokens []string, idx int) bool {
	for i := idx - 1; i >= 0; i-- {
		if tokens[i] == "|" {
			return true
		}
		if tokens[i] == ";" {
			return false
		}
	}
	return false
}

func isCommandSeparator(token string) bool {
	return token == "|" || token == ";" || token == "&&" || token == "||"
}

func nonInteractiveTerminalEnv() []string {
	return []string{
		"TERM=dumb",
		"PAGER=cat",
		"PSQL_PAGER=cat",
		"MYSQL_PAGER=cat",
		"LESS=-F -X",
	}
}

func shellSingleQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func (t *terminal) ReadFile(ctx context.Context, flowID int64, path string) (string, error) {
	containerName := PrimaryTerminalName(flowID)

	isRunning, err := t.dockerClient.IsContainerRunning(ctx, t.containerLID)
	if err != nil {
		return "", fmt.Errorf("runtime verification failed: %w", err)
	}
	if !isRunning {
		return "", fmt.Errorf("container runtime is not operational")
	}

	cwd := docker.WorkFolderPathInContainer
	escapedPath := strings.ReplaceAll(path, "'", "'\"'\"'")
	catCommand := fmt.Sprintf("cat '%s'", escapedPath)
	// Format read file command with styling
	styledCommand := fmt.Sprintf("%s $ %s%s%s%s", cwd, ansiColorInputCmd, catCommand, ansiColorReset, ansiLineTerminator)
	_, err = t.tlp.PutMsg(ctx, database.TermlogTypeStdin, styledCommand, t.containerID, t.taskID, t.subtaskID)
	if err != nil {
		return "", fmt.Errorf("failed to put terminal log (read file cmd): %w", err)
	}

	reader, stats, err := t.dockerClient.CopyFromContainer(ctx, containerName, path)
	if err != nil {
		return "", fmt.Errorf("failed to copy file: %w", err)
	}
	defer reader.Close()

	var buffer strings.Builder
	tarReader := tar.NewReader(reader)
	for {
		tarHeader, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to read tar header: %w", err)
		}

		if tarHeader.FileInfo().IsDir() {
			continue
		}

		if stats.Mode.IsDir() {
			buffer.WriteString("--------------------------------------------------\n")
			buffer.WriteString(
				fmt.Sprintf("'%s' file content (with size %d bytes) shown below:\n",
					tarHeader.Name, tarHeader.Size,
				),
			)
		}

		const maxReadFileSize int64 = 100 * 1024 * 1024 // 100 MB limit
		if tarHeader.Size > maxReadFileSize {
			return "", fmt.Errorf("file '%s' size %d exceeds maximum allowed size %d", tarHeader.Name, tarHeader.Size, maxReadFileSize)
		}
		if tarHeader.Size < 0 {
			return "", fmt.Errorf("file '%s' has invalid size %d", tarHeader.Name, tarHeader.Size)
		}

		var fileContent = make([]byte, tarHeader.Size)
		_, err = tarReader.Read(fileContent)
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("failed to read file '%s' content: %w", tarHeader.Name, err)
		}
		buffer.Write(fileContent)

		if stats.Mode.IsDir() {
			buffer.WriteString("\n\n")
		}
	}

	content := buffer.String()
	// Style file content output
	styledContent := fmt.Sprintf("%s%s%s%s", ansiColorSystemMsg, content, ansiColorReset, ansiLineTerminator)
	_, err = t.tlp.PutMsg(ctx, database.TermlogTypeStdout, styledContent, t.containerID, t.taskID, t.subtaskID)
	if err != nil {
		return "", fmt.Errorf("failed to put terminal log (read file content): %w", err)
	}

	return content, nil
}

func (t *terminal) WriteFile(ctx context.Context, flowID int64, content string, path string) (string, error) {
	containerName := PrimaryTerminalName(flowID)

	isRunning, err := t.dockerClient.IsContainerRunning(ctx, t.containerLID)
	if err != nil {
		return "", fmt.Errorf("container runtime check failed: %w", err)
	}
	if !isRunning {
		return "", fmt.Errorf("target container is not operational")
	}

	// Docker SDK requires TAR format for file transfer
	tarBuffer := &bytes.Buffer{}
	archiveWriter := tar.NewWriter(tarBuffer)
	defer archiveWriter.Close()

	filename := filepath.Base(path)
	fileDescriptor := &tar.Header{
		Name: filename,
		Mode: 0600,
		Size: int64(len(content)),
	}
	err = archiveWriter.WriteHeader(fileDescriptor)
	if err != nil {
		return "", fmt.Errorf("tar archive header generation failed: %w", err)
	}

	_, err = archiveWriter.Write([]byte(content))
	if err != nil {
		return "", fmt.Errorf("tar archive content serialization failed: %w", err)
	}

	err = archiveWriter.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close tar writer: %w", err)
	}

	dir := filepath.Dir(path)
	err = t.dockerClient.CopyToContainer(ctx, containerName, dir, tarBuffer, container.CopyToContainerOptions{
		AllowOverwriteDirWithFile: true,
	})
	if err != nil {
		return "", fmt.Errorf("container file transfer failed: %w", err)
	}

	// Format success message with styling
	successMsg := fmt.Sprintf("File successfully saved to %s", path)
	styledMsg := fmt.Sprintf("%s%s%s%s", ansiColorSystemMsg, successMsg, ansiColorReset, ansiLineTerminator)
	_, err = t.tlp.PutMsg(ctx, database.TermlogTypeStdin, styledMsg, t.containerID, t.taskID, t.subtaskID)
	if err != nil {
		return "", fmt.Errorf("failed to put terminal log (write file cmd): %w", err)
	}

	return fmt.Sprintf("Successfully wrote %d bytes to %s", len(content), path), nil
}

func PrimaryTerminalName(flowID int64) string {
	return fmt.Sprintf("pentagentx-terminal-%d", flowID)
}

func (t *terminal) IsAvailable() bool {
	return t.dockerClient != nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "... [truncated full size is " + strconv.Itoa(len(s)) + " bytes]"
}
