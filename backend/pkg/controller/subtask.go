package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"pentagi/pkg/database"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/orchestrator"
	"pentagi/pkg/providers"
)

type agentExecuteState string

const (
	agentStateUnspecified agentExecuteState = "AGENT_STATE_UNSPECIFIED"
	agentStateWorking     agentExecuteState = "AGENT_STATE_WORKING"
	agentStateCompleted   agentExecuteState = "AGENT_STATE_COMPLETED"
	agentStateFailed      agentExecuteState = "AGENT_STATE_FAILED"
)

type agentStateLogEntry struct {
	SchemaVersion string            `json:"schema_version"`
	MessageID     string            `json:"message_id"`
	Event         string            `json:"event"`
	Node          string            `json:"node"`
	Role          string            `json:"role"`
	AgentStatus   agentExecuteState `json:"agent_status"`
	TaskID        string            `json:"task_id"`
	ContextID     string            `json:"context_id"`
	SubtaskID     string            `json:"subtask_id"`
	Timestamp     string            `json:"timestamp"`
	Details       map[string]string `json:"details,omitempty"`
}

var agentStateSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(api[_-]?key|token|authorization|cookie|password)=\S+`),
	regexp.MustCompile(`(?i)"(api[_-]?key|token|authorization|cookie|password)"\s*:\s*"[^"]+"`),
	regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/-]+=*`),
	regexp.MustCompile(`sk-[A-Za-z0-9_-]{8,}`),
}

type TaskUpdater interface {
	SetStatus(ctx context.Context, status database.TaskStatus) error
}

type SubtaskWorker interface {
	GetMsgChainID() int64
	GetSubtaskID() int64
	GetTaskID() int64
	GetFlowID() int64
	GetUserID() int64
	GetTitle() string
	GetDescription() string
	IsCompleted() bool
	IsWaiting() bool
	GetStatus(ctx context.Context) (database.SubtaskStatus, error)
	SetStatus(ctx context.Context, status database.SubtaskStatus) error
	GetResult(ctx context.Context) (string, error)
	SetResult(ctx context.Context, result string) error
	PutInput(ctx context.Context, input string) error
	EnsurePrepared(ctx context.Context) (int64, error)
	StepPrimaryAgent(ctx context.Context) (*orchestrator.PrimaryAgentDecision, error)
	ExecuteAgent(ctx context.Context, agentType string, payload json.RawMessage) (*orchestrator.AgentExecutionResult, error)
	WritePrimaryAgentToolResult(ctx context.Context, agentType, toolCallID, result string) error
	Run(ctx context.Context) error
	Finish(ctx context.Context) error
}

type subtaskWorker struct {
	mx         *sync.RWMutex
	subtaskCtx *SubtaskContext
	updater    TaskUpdater
	completed  bool
	waiting    bool
}

func NewSubtaskWorker(
	ctx context.Context,
	taskCtx *TaskContext,
	id int64,
	title,
	description string,
	updater TaskUpdater,
) (SubtaskWorker, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "controller.NewSubtaskWorker")
	defer span.End()

	msgChainID, err := taskCtx.Provider.PrepareAgentChain(ctx, taskCtx.TaskID, id)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare primary agent chain for subtask %d: %w", id, err)
	}

	return &subtaskWorker{
		mx: &sync.RWMutex{},
		subtaskCtx: &SubtaskContext{
			MsgChainID:         msgChainID,
			SubtaskID:          id,
			SubtaskTitle:       title,
			SubtaskDescription: description,
			TaskContext:        *taskCtx,
		},
		updater:   updater,
		completed: false,
		waiting:   false,
	}, nil
}

func LoadSubtaskWorker(
	ctx context.Context,
	subtask database.Subtask,
	taskCtx *TaskContext,
	updater TaskUpdater,
) (SubtaskWorker, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "controller.LoadSubtaskWorker")
	defer span.End()

	var completed, waiting bool
	switch subtask.Status {
	case database.SubtaskStatusFinished, database.SubtaskStatusFailed:
		completed = true
	case database.SubtaskStatusWaiting:
		waiting = true
	case database.SubtaskStatusRunning:
		var err error
		// if subtask is running, it means that it was not finished by previous run
		// so we need to set it to created and continue from the beginning
		subtask, err = taskCtx.DB.UpdateSubtaskStatus(ctx, database.UpdateSubtaskStatusParams{
			Status: database.SubtaskStatusCreated,
			ID:     subtask.ID,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update subtask %d status to created: %w", subtask.ID, err)
		}
	case database.SubtaskStatusCreated:
		return nil, fmt.Errorf("subtask %d has created yet: %w", subtask.ID, ErrNothingToLoad)
	default:
		return nil, fmt.Errorf("unexpected subtask status: %s", subtask.Status)
	}

	msgChains, err := taskCtx.DB.GetSubtaskPrimaryMsgChains(ctx, database.Int64ToNullInt64(&subtask.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to get subtask primary msg chains for subtask %d: %w", subtask.ID, err)
	}

	if len(msgChains) == 0 {
		return nil, fmt.Errorf("subtask %d has no msg chains: %w", subtask.ID, ErrNothingToLoad)
	}

	return &subtaskWorker{
		mx: &sync.RWMutex{},
		subtaskCtx: &SubtaskContext{
			MsgChainID:         msgChains[0].ID,
			SubtaskID:          subtask.ID,
			SubtaskTitle:       subtask.Title,
			SubtaskDescription: subtask.Description,
			TaskContext:        *taskCtx,
		},
		updater:   updater,
		completed: completed,
		waiting:   waiting,
	}, nil
}

func (stw *subtaskWorker) GetMsgChainID() int64 {
	return stw.subtaskCtx.MsgChainID
}

func (stw *subtaskWorker) GetSubtaskID() int64 {
	return stw.subtaskCtx.SubtaskID
}

func (stw *subtaskWorker) GetTaskID() int64 {
	return stw.subtaskCtx.TaskID
}

func (stw *subtaskWorker) GetFlowID() int64 {
	return stw.subtaskCtx.FlowID
}

func (stw *subtaskWorker) GetUserID() int64 {
	return stw.subtaskCtx.UserID
}

func (stw *subtaskWorker) GetTitle() string {
	return stw.subtaskCtx.SubtaskTitle
}

func (stw *subtaskWorker) GetDescription() string {
	return stw.subtaskCtx.SubtaskDescription
}

func (stw *subtaskWorker) IsCompleted() bool {
	stw.mx.RLock()
	defer stw.mx.RUnlock()

	return stw.completed
}

func (stw *subtaskWorker) IsWaiting() bool {
	stw.mx.RLock()
	defer stw.mx.RUnlock()

	return stw.waiting
}

func (stw *subtaskWorker) GetStatus(ctx context.Context) (database.SubtaskStatus, error) {
	subtask, err := stw.subtaskCtx.DB.GetSubtask(ctx, stw.subtaskCtx.SubtaskID)
	if err != nil {
		return database.SubtaskStatusFailed, err
	}

	return subtask.Status, nil
}

func (stw *subtaskWorker) SetStatus(ctx context.Context, status database.SubtaskStatus) error {
	_, err := stw.subtaskCtx.DB.UpdateSubtaskStatus(ctx, database.UpdateSubtaskStatusParams{
		Status: status,
		ID:     stw.subtaskCtx.SubtaskID,
	})
	if err != nil {
		return fmt.Errorf("failed to set subtask %d status: %w", stw.subtaskCtx.SubtaskID, err)
	}

	stw.mx.Lock()
	defer stw.mx.Unlock()

	switch status {
	case database.SubtaskStatusRunning:
		stw.completed = false
		stw.waiting = false
		err = stw.updater.SetStatus(ctx, database.TaskStatusRunning)
	case database.SubtaskStatusWaiting:
		stw.completed = false
		stw.waiting = true
		err = stw.updater.SetStatus(ctx, database.TaskStatusWaiting)
	case database.SubtaskStatusFinished, database.SubtaskStatusFailed:
		stw.completed = true
		stw.waiting = false
		// statuses Finished and Failed will be produced by stack from Run function call
	default:
		// status Created is not possible to set by this call
		return fmt.Errorf("unsupported subtask status: %s", status)
	}
	if err != nil {
		return fmt.Errorf("failed to set task status in back propagation: %w", err)
	}

	return nil
}

func (stw *subtaskWorker) GetResult(ctx context.Context) (string, error) {
	subtask, err := stw.subtaskCtx.DB.GetSubtask(ctx, stw.subtaskCtx.SubtaskID)
	if err != nil {
		return "", err
	}

	return subtask.Result, nil
}

func (stw *subtaskWorker) SetResult(ctx context.Context, result string) error {
	_, err := stw.subtaskCtx.DB.UpdateSubtaskResult(ctx, database.UpdateSubtaskResultParams{
		Result: result,
		ID:     stw.subtaskCtx.SubtaskID,
	})
	if err != nil {
		return fmt.Errorf("failed to set subtask %d result: %w", stw.subtaskCtx.SubtaskID, err)
	}

	return nil
}

func (stw *subtaskWorker) PutInput(ctx context.Context, input string) error {
	if stw.IsCompleted() {
		return fmt.Errorf("subtask has already completed")
	}

	if !stw.IsWaiting() {
		return fmt.Errorf("subtask is not waiting, run first")
	}

	err := stw.subtaskCtx.Provider.PutInputToAgentChain(ctx, stw.subtaskCtx.MsgChainID, input)
	if err != nil {
		return fmt.Errorf("failed to put input for subtask %d: %w", stw.subtaskCtx.SubtaskID, err)
	}

	_, err = stw.subtaskCtx.MsgLog.PutSubtaskMsg(
		ctx,
		database.MsglogTypeInput,
		stw.subtaskCtx.TaskID,
		stw.subtaskCtx.SubtaskID,
		"", // thinking is empty because this is input
		input,
	)
	if err != nil {
		return fmt.Errorf("failed to put input for subtask %d: %w", stw.subtaskCtx.SubtaskID, err)
	}

	stw.mx.Lock()
	defer stw.mx.Unlock()

	stw.waiting = false

	return nil
}

func (stw *subtaskWorker) EnsurePrepared(ctx context.Context) (int64, error) {
	if stw.subtaskCtx.MsgChainID != 0 {
		return stw.subtaskCtx.MsgChainID, nil
	}

	msgChainID, err := stw.subtaskCtx.Provider.PrepareAgentChain(
		ctx,
		stw.subtaskCtx.TaskID,
		stw.subtaskCtx.SubtaskID,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare primary agent chain for subtask %d: %w", stw.subtaskCtx.SubtaskID, err)
	}

	stw.mx.Lock()
	stw.subtaskCtx.MsgChainID = msgChainID
	stw.mx.Unlock()

	return msgChainID, nil
}

func (stw *subtaskWorker) putAgentStateLog(
	ctx context.Context,
	node string,
	agentStatus agentExecuteState,
	fields map[string]string,
) error {
	details := make(map[string]string, len(fields))
	for key, value := range fields {
		value = trimAgentStateValue(value)
		if value == "" {
			continue
		}
		details[key] = value
	}
	if len(details) == 0 {
		details = nil
	}

	now := time.Now().UTC()
	entry := agentStateLogEntry{
		SchemaVersion: "agent-state.v1",
		MessageID:     fmt.Sprintf("agent_state_%d_%d_%d", stw.subtaskCtx.TaskID, stw.subtaskCtx.SubtaskID, now.UnixNano()),
		Event:         "status",
		Node:          node,
		Role:          agentRoleForNode(node),
		AgentStatus:   agentStatus,
		TaskID:        fmt.Sprintf("%d", stw.subtaskCtx.TaskID),
		ContextID:     fmt.Sprintf("flow_%d", stw.subtaskCtx.FlowID),
		SubtaskID:     fmt.Sprintf("%d", stw.subtaskCtx.SubtaskID),
		Timestamp:     now.Format(time.RFC3339Nano),
		Details:       details,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal agent state log for %s/%s: %w", node, agentStatus, err)
	}

	_, err = stw.subtaskCtx.MsgLog.PutSubtaskMsg(
		ctx,
		database.MsglogTypeThoughts,
		stw.subtaskCtx.TaskID,
		stw.subtaskCtx.SubtaskID,
		"",
		fmt.Sprintf("[agent-state] %s", string(data)),
	)
	if err != nil {
		return fmt.Errorf("failed to put agent state log for %s/%s: %w", node, agentStatus, err)
	}

	return nil
}

func agentRoleForNode(node string) string {
	switch strings.ToLower(strings.TrimSpace(node)) {
	case "primary_agent", "supervisor":
		return "Supervisor"
	case "designer":
		return "Designer"
	case "planner":
		return "Planner"
	case "generator", "coder":
		return "Generator"
	case "integrator":
		return "Integrator"
	case "reviewer":
		return "Reviewer"
	case "builder", "installer":
		return "Builder"
	case "tester":
		return "Tester"
	case "pentester":
		return "Pentester"
	default:
		return "Supervisor"
	}
}

func trimAgentStateValue(value string) string {
	const maxAgentStateValueLength = 512

	value = strings.TrimSpace(value)
	for _, pattern := range agentStateSecretPatterns {
		value = pattern.ReplaceAllString(value, "[REDACTED]")
	}
	if len(value) <= maxAgentStateValueLength {
		return value
	}

	return value[:maxAgentStateValueLength] + "..."
}

func (stw *subtaskWorker) StepPrimaryAgent(ctx context.Context) (*orchestrator.PrimaryAgentDecision, error) {
	if stw.IsCompleted() {
		return nil, fmt.Errorf("subtask has already completed")
	}

	if _, err := stw.EnsurePrepared(ctx); err != nil {
		return nil, err
	}

	status, err := stw.GetStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get subtask %d status: %w", stw.subtaskCtx.SubtaskID, err)
	}

	if status != database.SubtaskStatusRunning {
		if err := stw.SetStatus(ctx, database.SubtaskStatusRunning); err != nil {
			return nil, err
		}
	}

	_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateWorking, map[string]string{
		"state_event":  "started",
		"msg_chain_id": fmt.Sprintf("%d", stw.subtaskCtx.MsgChainID),
	})

	decision, err := stw.subtaskCtx.Provider.DecidePrimaryAgentStep(
		ctx,
		stw.subtaskCtx.TaskID,
		stw.subtaskCtx.SubtaskID,
		stw.subtaskCtx.MsgChainID,
	)
	if err != nil {
		_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateFailed, map[string]string{
			"state_event": "failed",
			"error_type":  "agent_execution_error",
			"reason":      err.Error(),
		})
		return nil, fmt.Errorf("failed to decide primary agent step for subtask %d: %w", stw.subtaskCtx.SubtaskID, err)
	}

	if decision.MsgChainID == 0 {
		decision.MsgChainID = stw.subtaskCtx.MsgChainID
	}

	switch decision.Action {
	case orchestrator.PrimaryAgentActionInputRequired:
		_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateCompleted, map[string]string{
			"state_event":  "input_required",
			"task_status":  "TASK_STATE_INPUT_REQUIRED",
			"message":      decision.Message,
			"tool_call_id": decision.ToolCallID,
		})
		if err := stw.SetStatus(ctx, database.SubtaskStatusWaiting); err != nil {
			return nil, err
		}
	case orchestrator.PrimaryAgentActionCompleted:
		_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateCompleted, map[string]string{
			"state_event":      "completed",
			"tool_call_id":     decision.ToolCallID,
			"result_available": fmt.Sprintf("%t", decision.Result != ""),
		})
		if err := stw.SetResult(ctx, decision.Result); err != nil {
			return nil, err
		}
		if err := stw.SetStatus(ctx, database.SubtaskStatusFinished); err != nil {
			return nil, err
		}
		if _, err := stw.subtaskCtx.MsgLog.PutSubtaskMsgResult(
			ctx,
			database.MsglogTypeReport,
			stw.subtaskCtx.TaskID,
			stw.subtaskCtx.SubtaskID,
			"",
			stw.subtaskCtx.SubtaskDescription,
			decision.Result,
			database.MsglogResultFormatMarkdown,
		); err != nil {
			return nil, fmt.Errorf("failed to put subtask %d report: %w", stw.subtaskCtx.SubtaskID, err)
		}
	case orchestrator.PrimaryAgentActionFailed:
		_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateFailed, map[string]string{
			"state_event":  "failed",
			"error_type":   "agent_execution_error",
			"tool_call_id": decision.ToolCallID,
			"reason":       decision.Error,
		})
		if err := stw.SetResult(ctx, decision.Error); err != nil {
			return nil, err
		}
		if err := stw.SetStatus(ctx, database.SubtaskStatusFailed); err != nil {
			return nil, err
		}
		if _, err := stw.subtaskCtx.MsgLog.PutSubtaskMsgResult(
			ctx,
			database.MsglogTypeReport,
			stw.subtaskCtx.TaskID,
			stw.subtaskCtx.SubtaskID,
			"",
			stw.subtaskCtx.SubtaskDescription,
			decision.Error,
			database.MsglogResultFormatMarkdown,
		); err != nil {
			return nil, fmt.Errorf("failed to put failed report for subtask %d: %w", stw.subtaskCtx.SubtaskID, err)
		}
	case orchestrator.PrimaryAgentActionCallAgent:
		_ = stw.putAgentStateLog(ctx, "primary_agent", agentStateWorking, map[string]string{
			"state_event":    "delegated",
			"delegated_node": decision.AgentType,
			"delegated_role": agentRoleForNode(decision.AgentType),
			"tool_call_id":   decision.ToolCallID,
		})
	}

	return decision, nil
}

func (stw *subtaskWorker) ExecuteAgent(
	ctx context.Context,
	agentType string,
	payload json.RawMessage,
) (*orchestrator.AgentExecutionResult, error) {
	_ = stw.putAgentStateLog(ctx, agentType, agentStateWorking, map[string]string{
		"state_event": "started",
	})

	result, err := stw.subtaskCtx.Provider.ExecuteDelegatedAgent(
		ctx,
		stw.subtaskCtx.TaskID,
		stw.subtaskCtx.SubtaskID,
		agentType,
		payload,
	)
	if err != nil {
		_ = stw.putAgentStateLog(ctx, agentType, agentStateFailed, map[string]string{
			"state_event": "failed",
			"error_type":  "agent_execution_error",
			"reason":      err.Error(),
		})
		return nil, err
	}

	if result.Success {
		_ = stw.putAgentStateLog(ctx, agentType, agentStateCompleted, map[string]string{
			"state_event":      "completed",
			"result_available": fmt.Sprintf("%t", result.Result != ""),
		})
	} else {
		_ = stw.putAgentStateLog(ctx, agentType, agentStateFailed, map[string]string{
			"state_event": "failed",
			"error_type":  "agent_execution_error",
			"reason":      result.Error,
		})
	}

	return result, nil
}

func (stw *subtaskWorker) WritePrimaryAgentToolResult(
	ctx context.Context,
	agentType, toolCallID, result string,
) error {
	if err := stw.subtaskCtx.Provider.WritePrimaryAgentToolResult(
		ctx,
		stw.subtaskCtx.MsgChainID,
		agentType,
		toolCallID,
		result,
	); err != nil {
		_ = stw.putAgentStateLog(ctx, agentType, agentStateFailed, map[string]string{
			"state_event":  "writeback_failed",
			"error_type":   "writeback_error",
			"tool_call_id": toolCallID,
			"reason":       err.Error(),
		})
		return err
	}

	_ = stw.putAgentStateLog(ctx, agentType, agentStateCompleted, map[string]string{
		"state_event":      "writeback_completed",
		"tool_call_id":     toolCallID,
		"result_available": fmt.Sprintf("%t", result != ""),
	})

	return nil
}

func (stw *subtaskWorker) Run(ctx context.Context) error {
	if stw.IsCompleted() {
		return fmt.Errorf("subtask has already completed")
	}

	if stw.IsWaiting() {
		return fmt.Errorf("subtask is waiting, put input first")
	}

	if err := stw.SetStatus(ctx, database.SubtaskStatusRunning); err != nil {
		return err
	}

	var (
		taskID     = stw.subtaskCtx.TaskID
		subtaskID  = stw.subtaskCtx.SubtaskID
		msgChainID = stw.subtaskCtx.MsgChainID
	)

	if err := stw.subtaskCtx.Provider.EnsureChainConsistency(ctx, msgChainID); err != nil {
		return fmt.Errorf("failed to ensure chain consistency for subtask %d: %w", subtaskID, err)
	}

	performResult, err := stw.subtaskCtx.Provider.PerformAgentChain(ctx, taskID, subtaskID, msgChainID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			ctx = context.Background()
		}
		errChainConsistency := stw.subtaskCtx.Provider.EnsureChainConsistency(ctx, msgChainID)
		if errChainConsistency != nil {
			err = errors.Join(err, errChainConsistency)
		}
		_ = stw.SetStatus(ctx, database.SubtaskStatusWaiting)
		return fmt.Errorf("failed to perform agent chain for subtask %d: %w", subtaskID, err)
	}

	switch performResult {
	case providers.PerformResultWaiting:
		if err := stw.SetStatus(ctx, database.SubtaskStatusWaiting); err != nil {
			return err
		}
	case providers.PerformResultDone:
		if err := stw.SetStatus(ctx, database.SubtaskStatusFinished); err != nil {
			return fmt.Errorf("failed to set subtask %d status to finished: %w", subtaskID, err)
		}
	case providers.PerformResultError:
		if err := stw.SetStatus(ctx, database.SubtaskStatusFailed); err != nil {
			return fmt.Errorf("failed to set subtask %d status to failed: %w", subtaskID, err)
		}
	default:
		return fmt.Errorf("unknown perform result: %d", performResult)
	}

	return nil
}

func (stw *subtaskWorker) Finish(ctx context.Context) error {
	if stw.IsCompleted() {
		return fmt.Errorf("subtask has already completed")
	}

	if err := stw.SetStatus(ctx, database.SubtaskStatusFinished); err != nil {
		return err
	}

	return nil
}
