package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"pentagi/pkg/cast"
	"pentagi/pkg/csum"
	"pentagi/pkg/database"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/orchestrator"
	"pentagi/pkg/providers/pconfig"
	"pentagi/pkg/templates"
	"pentagi/pkg/tools"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/langchaingo/llms"
)

func toolNameToAgentType(toolName string) (string, error) {
	switch toolName {
	case tools.CoderToolName:
		return "coder", nil
	case tools.IntegratorToolName:
		return "integrator", nil
	case tools.PentesterToolName:
		return "pentester", nil
	case tools.TesterToolName:
		return "tester", nil
	case tools.SearchToolName:
		return "searcher", nil
	case tools.MaintenanceToolName:
		return "installer", nil
	case tools.MemoristToolName:
		return "memorist", nil
	case tools.AdviceToolName:
		return "adviser", nil
	default:
		return "", fmt.Errorf("unsupported primary agent tool %q", toolName)
	}
}

func agentTypeToToolName(agentType string) (string, error) {
	switch agentType {
	case "builder":
		return tools.MaintenanceToolName, nil
	case "generator":
		return tools.CoderToolName, nil
	case "coder":
		return tools.CoderToolName, nil
	case "integrator":
		return tools.IntegratorToolName, nil
	case "pentester":
		return tools.PentesterToolName, nil
	case "tester":
		return tools.TesterToolName, nil
	case "researcher":
		return tools.SearchToolName, nil
	case "searcher":
		return tools.SearchToolName, nil
	case "installer":
		return tools.MaintenanceToolName, nil
	case "memorist":
		return tools.MemoristToolName, nil
	case "adviser":
		return tools.AdviceToolName, nil
	case "reviewer":
		return tools.ReviewResultToolName, nil
	case "reporter":
		return tools.ReportResultToolName, nil
	case "designer":
		return tools.ScopeContractToolName, nil
	case "planner":
		return tools.TodoListToolName, nil
	default:
		return "", fmt.Errorf("unsupported delegated agent type %q", agentType)
	}
}

func (fp *flowProvider) DecidePrimaryAgentStep(
	ctx context.Context,
	taskID, subtaskID, msgChainID int64,
) (*orchestrator.PrimaryAgentDecision, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.DecidePrimaryAgentStep")
	defer span.End()

	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"flow_id":      fp.flowID,
		"task_id":      taskID,
		"subtask_id":   subtaskID,
		"msg_chain_id": msgChainID,
		"agent":        pconfig.OptionsTypePrimaryAgent,
	})

	msgChain, err := fp.db.GetMsgChain(ctx, msgChainID)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary agent msg chain %d: %w", msgChainID, err)
	}

	var chain []llms.MessageContent
	if err := json.Unmarshal(msgChain.Chain, &chain); err != nil {
		return nil, fmt.Errorf("failed to unmarshal primary agent msg chain %d: %w", msgChainID, err)
	}

	decision := &orchestrator.PrimaryAgentDecision{
		MsgChainID: msgChainID,
	}

	barrierHandler := func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		switch name {
		case tools.FinalyToolName:
			var done tools.Done
			if err := json.Unmarshal(args, &done); err != nil {
				return "", fmt.Errorf("failed to unmarshal done result: %w", err)
			}

			if done.Success {
				decision.Action = orchestrator.PrimaryAgentActionCompleted
				decision.Result = done.Result
				return done.Result, nil
			}

			decision.Action = orchestrator.PrimaryAgentActionFailed
			decision.Error = done.Result
			if decision.Error == "" {
				decision.Error = "primary_agent reported failure"
			}
			return decision.Error, nil
		case tools.AskUserToolName:
			var askUser tools.AskUser
			if err := json.Unmarshal(args, &askUser); err != nil {
				return "", fmt.Errorf("failed to unmarshal ask user result: %w", err)
			}

			decision.Action = orchestrator.PrimaryAgentActionInputRequired
			decision.Message = askUser.Message
			return askUser.Message, nil
		default:
			return "", fmt.Errorf("unsupported barrier tool %q", name)
		}
	}

	buildAgentHandler := func(agentType string) tools.ExecutorHandler {
		return func(ctx context.Context, name string, args json.RawMessage) (string, error) {
			decision.Action = orchestrator.PrimaryAgentActionCallAgent
			decision.AgentType = agentType
			decision.Payload = append(json.RawMessage(nil), args...)
			return fmt.Sprintf("delegated to %s; waiting for orchestrator writeback", agentType), nil
		}
	}

	executor, err := fp.executor.GetPrimaryDecisionExecutor(tools.PrimaryExecutorConfig{
		TaskID:     taskID,
		SubtaskID:  subtaskID,
		Barrier:    barrierHandler,
		Adviser:    buildAgentHandler("adviser"),
		Coder:      buildAgentHandler("coder"),
		Installer:  buildAgentHandler("installer"),
		Integrator: buildAgentHandler("integrator"),
		Memorist:   buildAgentHandler("memorist"),
		Pentester:  buildAgentHandler("pentester"),
		Searcher:   buildAgentHandler("searcher"),
		Tester:     buildAgentHandler("tester"),
		Summarizer: fp.GetSummarizeResultHandler(&taskID, &subtaskID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create primary decision executor: %w", err)
	}

	executionContext, err := fp.getExecutionContext(ctx, &taskID, &subtaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution context: %w", err)
	}

	lastUpdateTime := time.Now()
	rollLastUpdateTime := func() float64 {
		durationDelta := time.Since(lastUpdateTime).Seconds()
		lastUpdateTime = time.Now()
		return durationDelta
	}

	ctx = tools.PutAgentContext(ctx, database.MsgchainTypePrimaryAgent)
	result, err := fp.callWithRetries(
		ctx,
		pconfig.OptionsTypePrimaryAgent,
		msgChainID,
		&taskID,
		&subtaskID,
		chain,
		executor,
		executionContext,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call primary agent for single-step decision: %w", err)
	}

	if err := fp.updateMsgChainUsage(
		ctx,
		msgChainID,
		pconfig.OptionsTypePrimaryAgent,
		result.info,
		rollLastUpdateTime(),
	); err != nil {
		return nil, fmt.Errorf("failed to update primary agent msg chain usage: %w", err)
	}

	if len(result.funcCalls) == 0 {
		return nil, fmt.Errorf("primary agent returned no structured tool call")
	}

	if len(result.funcCalls) > 1 {
		logger.WithField("tool_call_count", len(result.funcCalls)).Warn(
			"primary agent returned multiple tool calls, only the first one will be orchestrated",
		)
	}

	selectedToolCall := result.funcCalls[0]
	selectedResult := &callResult{
		streamID:  result.streamID,
		funcCalls: []llms.ToolCall{selectedToolCall},
		thinking:  result.thinking,
		content:   result.content,
		info:      result.info,
	}

	msg := llms.MessageContent{Role: llms.ChatMessageTypeAI}
	if selectedResult.content != "" || !selectedResult.thinking.IsEmpty() {
		msg.Parts = append(msg.Parts, llms.TextPartWithReasoning(selectedResult.content, selectedResult.thinking))
	}
	msg.Parts = append(msg.Parts, selectedToolCall)
	chain = append(chain, msg)

	if err := fp.updateMsgChain(
		ctx,
		pconfig.OptionsTypePrimaryAgent,
		msgChainID,
		chain,
		rollLastUpdateTime(),
	); err != nil {
		return nil, fmt.Errorf("failed to append decision tool call to primary agent chain: %w", err)
	}

	response, err := fp.execToolCall(
		ctx,
		pconfig.OptionsTypePrimaryAgent,
		msgChainID,
		0,
		selectedResult,
		fp.buildMonitor(),
		&repeatingDetector{},
		executor,
		&taskID,
		&subtaskID,
		chain,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute decision barrier tool call: %w", err)
	}

	if decision.ToolCallID == "" {
		decision.ToolCallID = selectedToolCall.ID
	}

	chain = append(chain, llms.MessageContent{
		Role: llms.ChatMessageTypeTool,
		Parts: []llms.ContentPart{
			llms.ToolCallResponse{
				ToolCallID: selectedToolCall.ID,
				Name:       selectedToolCall.FunctionCall.Name,
				Content:    response,
			},
		},
	})

	if err := fp.updateMsgChain(
		ctx,
		pconfig.OptionsTypePrimaryAgent,
		msgChainID,
		chain,
		rollLastUpdateTime(),
	); err != nil {
		return nil, fmt.Errorf("failed to append decision tool response to primary agent chain: %w", err)
	}

	if decision.Action == "" {
		return nil, fmt.Errorf("primary agent decision handler produced empty action")
	}

	return decision, nil
}

func (fp *flowProvider) ExecuteDelegatedAgent(
	ctx context.Context,
	taskID, subtaskID int64,
	agentType string,
	payload json.RawMessage,
) (*orchestrator.AgentExecutionResult, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.ExecuteDelegatedAgent")
	defer span.End()

	toolName, err := agentTypeToToolName(agentType)
	if err != nil {
		return nil, err
	}

	// In multi-agent mode (designer/planner/supervisor), tasks use the todo system
	// and have no subtasks. When subtaskID is 0, pass nil so handlers skip the
	// subtask DB query instead of trying to fetch a non-existent row.
	taskIDPtr := &taskID
	var subtaskIDPtr *int64
	if subtaskID > 0 {
		subtaskIDPtr = &subtaskID
	}

	var handler tools.ExecutorHandler
	switch agentType {
	case "builder":
		handler, err = fp.GetInstallerHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "generator":
		handler, err = fp.GetCoderHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "coder":
		handler, err = fp.GetCoderHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "integrator":
		handler, err = fp.GetIntegratorHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "pentester":
		handler, err = fp.GetPentesterHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "tester":
		handler, err = fp.GetTesterHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "researcher":
		handler, err = fp.GetSubtaskSearcherHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "searcher":
		handler, err = fp.GetSubtaskSearcherHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "installer":
		handler, err = fp.GetInstallerHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "memorist":
		handler, err = fp.GetMemoristHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "adviser":
		handler, err = fp.GetAskAdviceHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "reviewer":
		handler, err = fp.GetReviewerHandler(ctx, taskIDPtr, subtaskIDPtr)
	case "reporter":
		handler, err = fp.GetReporterHandler(ctx, taskIDPtr, subtaskIDPtr)
	default:
		err = fmt.Errorf("unsupported delegated agent type %q", agentType)
	}
	if err != nil {
		return nil, err
	}

	result, execErr := handler(ctx, toolName, payload)
	if execErr != nil {
		return &orchestrator.AgentExecutionResult{
			AgentType: agentType,
			Success:   false,
			Error:     execErr.Error(),
		}, nil
	}

	return &orchestrator.AgentExecutionResult{
		AgentType: agentType,
		Success:   true,
		Result:    result,
	}, nil
}

func (fp *flowProvider) WritePrimaryAgentToolResult(
	ctx context.Context,
	msgChainID int64,
	agentType, toolCallID, result string,
) error {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.WritePrimaryAgentToolResult")
	defer span.End()

	toolName, err := agentTypeToToolName(agentType)
	if err != nil {
		return err
	}

	msgChain, err := fp.db.GetMsgChain(ctx, msgChainID)
	if err != nil {
		return fmt.Errorf("failed to get primary agent msg chain %d: %w", msgChainID, err)
	}

	var chain []llms.MessageContent
	if err := json.Unmarshal(msgChain.Chain, &chain); err != nil {
		return fmt.Errorf("failed to unmarshal primary agent msg chain %d: %w", msgChainID, err)
	}

	chain, err = fp.updateMsgChainResult(chain, toolName, result)
	if err != nil {
		return fmt.Errorf("failed to update primary agent tool response: %w", err)
	}

	if err := fp.updateMsgChain(ctx, pconfig.OptionsTypePrimaryAgent, msgChainID, chain, 0); err != nil {
		return fmt.Errorf("failed to persist updated primary agent msg chain: %w", err)
	}

	if toolCallID == "" {
		return nil
	}

	tc, err := fp.db.GetCallToolcall(ctx, toolCallID)
	if err != nil {
		return nil
	}

	if _, err := fp.db.UpdateToolcallFinishedResult(ctx, database.UpdateToolcallFinishedResultParams{
		Result:          result,
		DurationSeconds: 0,
		ID:              tc.ID,
	}); err != nil {
		return fmt.Errorf("failed to update delegated primary toolcall result: %w", err)
	}

	return nil
}

// nodeRoleToMsgChainType maps a node role to the correct MsgchainType.
func nodeRoleToMsgChainType(nodeRole string) database.MsgchainType {
	switch nodeRole {
	case "designer":
		return database.MsgchainTypeDesigner
	case "supervisor":
		return database.MsgchainTypeSupervisor
	case "planner":
		return database.MsgchainTypePlanner
	default:
		return database.MsgchainTypePrimaryAgent
	}
}

// DecideSupervisorStep executes a single LLM step for a multi-agent supervisor node.
// nodeRole is "designer", "planner", or "supervisor" — selects the prompt and tool set.
// msgChainID is the existing chain to restore; if 0, a new chain is created.
func (fp *flowProvider) DecideSupervisorStep(ctx context.Context, taskID int64, nodeRole string, msgChainID int64) (*orchestrator.SupervisorDecision, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.DecideSupervisorStep")
	defer span.End()

	optAgentType := pconfig.ProviderOptionsType(nodeRole)
	mcType := nodeRoleToMsgChainType(nodeRole)
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"flow_id":      fp.flowID,
		"task_id":      taskID,
		"agent":        optAgentType,
		"msg_chain_id": msgChainID,
	})

	decision := &orchestrator.SupervisorDecision{}

	// Build tool definitions and handlers based on role
	defs, handlers, barriers, err := fp.buildSupervisorTools(ctx, taskID, nodeRole, decision)
	if err != nil {
		return nil, fmt.Errorf("failed to build %s tools: %w", nodeRole, err)
	}

	executor, err := fp.executor.GetCustomExecutor(tools.CustomExecutorConfig{
		TaskID:      &taskID,
		Definitions: defs,
		Handlers:    handlers,
		Barriers:    barriers,
		Summarizer:  fp.GetSummarizeResultHandler(&taskID, nil),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create %s executor: %w", nodeRole, err)
	}

	executionContext, err := fp.getExecutionContextByTask(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution context: %w", err)
	}

	// Render system prompt
	promptType := templates.PromptTypeSupervisor
	questionType := templates.PromptTypeQuestionSupervisor
	if nodeRole == "designer" {
		promptType = templates.PromptTypeDesigner
		questionType = templates.PromptTypeQuestionDesigner
	} else if nodeRole == "planner" {
		promptType = templates.PromptTypePlanner
		questionType = templates.PromptTypeQuestionPlanner
	}

	barrierToolNames := executor.GetBarrierToolNames()

	// Build template variables — common to all roles
	templateVars := map[string]string{
		"BarrierTools":            strings.Join(barrierToolNames, ", "),
		"CurrentTime":             time.Now().Format(time.RFC3339),
		"Lang":                    fp.Language(),
		"ExecutionContext":        executionContext,
		"ToolPlaceholder":         ToolPlaceholder,
		"SummarizationToolName":   cast.SummarizationToolName,
		"SummarizedContentPrefix": csum.SummarizedContentPrefix,
	}

	// Role-specific variables
	switch nodeRole {
	case "designer":
		templateVars["SearchToolName"] = tools.SearchToolName
		templateVars["MemoristToolName"] = tools.MemoristToolName
		templateVars["AskUserToolName"] = tools.AskUserToolName
		templateVars["FinalyToolName"] = tools.FinalyToolName

	case "supervisor":
		templateVars["AskUserToolName"] = tools.AskUserToolName
		templateVars["FinalyToolName"] = tools.FinalyToolName

	case "planner":
		// Check if todos already exist to decide mode
		accessor, ok := fp.db.(dbAccessor)
		if !ok {
			return nil, fmt.Errorf("db does not implement dbAccessor for planner mode detection")
		}
		maQ := database.NewMultiAgentQueries(accessor.DB())
		existingTodos, todosErr := maQ.GetTodosByTaskID(ctx, taskID)

		if todosErr != nil {

			logger.WithError(todosErr).Warn("failed to check existing todos, defaulting to generate mode")

		}

		if todosErr == nil && len(existingTodos) > 0 {

			templateVars["Mode"] = "refine"

		} else {

			templateVars["Mode"] = "generate"

		}

		templateVars["TodoListToolName"] = tools.TodoListToolName
		templateVars["TodoPatchToolName"] = tools.TodoPatchToolName
		templateVars["N"] = "10"
		templateVars["SearchToolName"] = tools.SearchToolName
		templateVars["TerminalToolName"] = tools.TerminalToolName
		templateVars["FileToolName"] = tools.FileToolName
		templateVars["BrowserToolName"] = tools.BrowserToolName
		templateVars["DockerImage"] = "vxcontrol/kali-linux"
	}

	systemPrompt, err := fp.prompter.RenderTemplate(promptType, templateVars)
	if err != nil {
		return nil, fmt.Errorf("failed to render %s system prompt: %w", nodeRole, err)
	}

	// Restore existing chain or create a new one
	var chain []llms.MessageContent
	var msgChain database.Msgchain

	if msgChainID > 0 {
		// Restore existing chain — supervisor/planner need history from previous rounds
		msgChain, err = fp.db.GetMsgChain(ctx, msgChainID)
		if err != nil {
			return nil, fmt.Errorf("failed to get %s msg chain %d: %w", nodeRole, msgChainID, err)
		}
		if err := json.Unmarshal(msgChain.Chain, &chain); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %s msg chain %d: %w", nodeRole, msgChainID, err)
		}

		// For returning rounds add a continuation prompt
		if nodeRole == "designer" {
			continuation := "The user has provided their response. Use this information to finalize the scope contract."
			chain = append(chain, llms.TextParts(llms.ChatMessageTypeHuman, continuation))
		} else if nodeRole == "planner" {
			continuation := "The supervisor has returned to you for plan refinement. Review the current state and update the todo plan as needed using the todo_list or todo_patch tools."
			chain = append(chain, llms.TextParts(llms.ChatMessageTypeHuman, continuation))
		}
	} else {
		// First call — build initial chain with system prompt and user input
		chain = []llms.MessageContent{
			llms.TextParts(llms.ChatMessageTypeSystem, systemPrompt),
		}

		task, err := fp.db.GetTask(ctx, taskID)
		if err != nil {
			return nil, fmt.Errorf("failed to get task %d: %w", taskID, err)
		}
		// Build question prompt with role-appropriate variables
		var questionParams map[string]any
		if nodeRole == "planner" {
			// Planner question template needs nested Task object
			questionParams = map[string]any{
				"Mode": templateVars["Mode"],
				"Task": map[string]string{
					"ID":     fmt.Sprintf("%d", task.ID),
					"Input":  task.Input,
					"Title":  task.Input,
					"Status": string(task.Status),
					"Result": task.Result,
				},
			}
			// Load scope_contract from multi-agent extension if available
			accessor, ok := fp.db.(dbAccessor)
			if !ok {
				return nil, fmt.Errorf("db does not implement dbAccessor for scope contract loading")
			}
			maQueries := database.NewMultiAgentQueries(accessor.DB())
			ext, extErr := maQueries.GetTaskExtension(ctx, task.ID)
			if extErr == nil && ext != nil && len(ext.ScopeContract) > 0 {
				questionParams["ScopeContract"] = string(ext.ScopeContract)
				questionParams["SharedState"] = string(ext.SharedState)
			}
		} else {
			// Designer and Supervisor use simple Question variable
			questionParams = map[string]any{
				"Question": task.Input,
			}
		}
		questionPrompt, _ := fp.prompter.RenderTemplate(questionType, questionParams)
		if questionPrompt != "" {
			chain = append(chain, llms.TextParts(llms.ChatMessageTypeHuman, questionPrompt))
		}

		chainBlob, err := json.Marshal(chain)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal chain: %w", err)
		}

		msgChain, err = fp.db.CreateMsgChain(ctx, database.CreateMsgChainParams{
			Type:          mcType,
			Model:         fp.Model(optAgentType),
			ModelProvider: string(fp.Type()),
			Chain:         chainBlob,
			FlowID:        fp.flowID,
			TaskID:        database.Int64ToNullInt64(&taskID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create msg chain: %w", err)
		}
	}

	lastUpdateTime := time.Now()
	rollLastUpdateTime := func() float64 {
		delta := time.Since(lastUpdateTime).Seconds()
		lastUpdateTime = time.Now()
		return delta
	}

	ctx = tools.PutAgentContext(ctx, mcType)
	result, err := fp.callWithRetries(
		ctx,
		optAgentType,
		msgChain.ID,
		&taskID,
		nil,
		chain,
		executor,
		executionContext,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call %s LLM: %w", nodeRole, err)
	}

	if err := fp.updateMsgChainUsage(ctx, msgChain.ID, optAgentType, result.info, rollLastUpdateTime()); err != nil {
		logger.WithError(err).Warn("failed to update msg chain usage")
	}

	if len(result.funcCalls) == 0 {
		return nil, fmt.Errorf("%s returned no structured tool call", nodeRole)
	}

	if len(result.funcCalls) > 1 {
		logger.WithField("tool_call_count", len(result.funcCalls)).Warn("multiple tool calls, only first used")
	}

	selectedToolCall := result.funcCalls[0]

	// Append AI message with tool call
	msg := llms.MessageContent{Role: llms.ChatMessageTypeAI}
	if result.content != "" || !result.thinking.IsEmpty() {
		msg.Parts = append(msg.Parts, llms.TextPartWithReasoning(result.content, result.thinking))
	}
	msg.Parts = append(msg.Parts, selectedToolCall)
	chain = append(chain, msg)

	if err := fp.updateMsgChain(ctx, optAgentType, msgChain.ID, chain, rollLastUpdateTime()); err != nil {
		logger.WithError(err).Warn("failed to append tool call to chain")
	}

	// Execute the tool call (triggers handler that populates decision)
	selectedResult := &callResult{
		streamID:  result.streamID,
		funcCalls: []llms.ToolCall{selectedToolCall},
		thinking:  result.thinking,
		content:   result.content,
		info:      result.info,
	}

	response, err := fp.execToolCall(
		ctx,
		optAgentType,
		msgChain.ID,
		0,
		selectedResult,
		fp.buildMonitor(),
		&repeatingDetector{},
		executor,
		&taskID,
		nil,
		chain,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute %s tool call: %w", nodeRole, err)
	}

	decision.ToolCallID = selectedToolCall.ID
	decision.MsgChainID = msgChain.ID

	// Append tool response
	chain = append(chain, llms.MessageContent{
		Role: llms.ChatMessageTypeTool,
		Parts: []llms.ContentPart{
			llms.ToolCallResponse{
				ToolCallID: selectedToolCall.ID,
				Name:       selectedToolCall.FunctionCall.Name,
				Content:    response,
			},
		},
	})

	if err := fp.updateMsgChain(ctx, optAgentType, msgChain.ID, chain, rollLastUpdateTime()); err != nil {
		logger.WithError(err).Warn("failed to append tool response to chain")
	}

	// If action is empty, check whether a barrier tool was called
	if decision.Action == "" {
		toolName := selectedToolCall.FunctionCall.Name
		if executor.IsBarrierFunction(toolName) {
			return nil, fmt.Errorf("%s barrier tool %q was called but decision handler produced empty action", nodeRole, toolName)
		}
	}

	// If action is empty (non-barrier tool was called, e.g. search), loop back to LLM
	maxLoops := 3
	for loop := 0; decision.Action == "" && loop < maxLoops; loop++ {
		// Reset decision fields to prevent stale state from previous iterations.
		// Non-barrier tool handlers (search, memorist) do not modify decision,
		// but a future handler might accidentally set a field — guard against that.
		decision.AgentRole = ""
		decision.Payload = nil
		decision.TodoID = ""
		decision.Message = ""
		decision.Result = ""
		decision.Error = ""

		logger.WithField("loop", loop).WithField("tool", selectedToolCall.FunctionCall.Name).Info("non-barrier tool called, retrying LLM")

		result, err = fp.callWithRetries(
			ctx,
			optAgentType,
			msgChain.ID,
			&taskID,
			nil,
			chain,
			executor,
			executionContext,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to call %s LLM (loop %d): %w", nodeRole, loop, err)
		}

		if err := fp.updateMsgChainUsage(ctx, msgChain.ID, optAgentType, result.info, rollLastUpdateTime()); err != nil {
			logger.WithError(err).Warn("failed to update msg chain usage")
		}

		if len(result.funcCalls) == 0 {
			return nil, fmt.Errorf("%s returned no structured tool call (loop %d)", nodeRole, loop)
		}

		selectedToolCall = result.funcCalls[0]
		msg = llms.MessageContent{Role: llms.ChatMessageTypeAI}
		if result.content != "" || !result.thinking.IsEmpty() {
			msg.Parts = append(msg.Parts, llms.TextPartWithReasoning(result.content, result.thinking))
		}
		msg.Parts = append(msg.Parts, selectedToolCall)
		chain = append(chain, msg)

		if err := fp.updateMsgChain(ctx, optAgentType, msgChain.ID, chain, rollLastUpdateTime()); err != nil {
			logger.WithError(err).Warn("failed to append tool call to chain")
		}

		selectedResult = &callResult{
			streamID:  result.streamID,
			funcCalls: []llms.ToolCall{selectedToolCall},
			thinking:  result.thinking,
			content:   result.content,
			info:      result.info,
		}

		response, err = fp.execToolCall(
			ctx,
			optAgentType,
			msgChain.ID,
			0,
			selectedResult,
			fp.buildMonitor(),
			&repeatingDetector{},
			executor,
			&taskID,
			nil,
			chain,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to execute %s tool call (loop %d): %w", nodeRole, loop, err)
		}

		decision.ToolCallID = selectedToolCall.ID

		chain = append(chain, llms.MessageContent{
			Role: llms.ChatMessageTypeTool,
			Parts: []llms.ContentPart{
				llms.ToolCallResponse{
					ToolCallID: selectedToolCall.ID,
					Name:       selectedToolCall.FunctionCall.Name,
					Content:    response,
				},
			},
		})

		if err := fp.updateMsgChain(ctx, optAgentType, msgChain.ID, chain, rollLastUpdateTime()); err != nil {
			logger.WithError(err).Warn("failed to append tool response to chain")
		}
	}

	if decision.Action == "" {
		return nil, fmt.Errorf("%s decision handler still has empty action after %d retries", nodeRole, maxLoops)
	}

	return decision, nil
}

// buildSupervisorTools builds tool definitions/handlers for designer or supervisor nodes.
func (fp *flowProvider) buildSupervisorTools(
	ctx context.Context,
	taskID int64,
	nodeRole string,
	decision *orchestrator.SupervisorDecision,
) ([]llms.FunctionDefinition, map[string]tools.ExecutorHandler, []string, error) {
	var defs []llms.FunctionDefinition
	handlers := make(map[string]tools.ExecutorHandler)
	var barriers []string

	// Common barrier tools: done, ask_user
	barriers = append(barriers, tools.FinalyToolName, tools.AskUserToolName)

	doneDef := tools.GetRegistryDefinitions()[tools.FinalyToolName]
	askDef := tools.GetRegistryDefinitions()[tools.AskUserToolName]
	defs = append(defs, doneDef, askDef)

	handlers[tools.FinalyToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		var done tools.Done
		if err := json.Unmarshal(args, &done); err != nil {
			return "", fmt.Errorf("failed to unmarshal done: %w", err)
		}
		if done.Success {
			decision.Action = orchestrator.SupervisorActionCompleted
			decision.Result = done.Result
		} else {
			decision.Action = orchestrator.SupervisorActionFailed
			decision.Error = done.Result
			if decision.Error == "" {
				decision.Error = nodeRole + " reported failure"
			}
		}
		return done.Result, nil
	}

	handlers[tools.AskUserToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		var ask tools.AskUser
		if err := json.Unmarshal(args, &ask); err != nil {
			return "", fmt.Errorf("failed to unmarshal ask_user: %w", err)
		}
		decision.Action = orchestrator.SupervisorActionInputRequired
		decision.Message = ask.Message
		return ask.Message, nil
	}

	if nodeRole == "designer" {
		// Designer produces scope_contract (barrier)
		scDef := tools.GetRegistryDefinitions()[tools.ScopeContractToolName]
		defs = append(defs, scDef)
		barriers = append(barriers, tools.ScopeContractToolName)

		handlers[tools.ScopeContractToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
			decision.Action = orchestrator.SupervisorActionCompleted
			decision.Result = string(args)
			return "scope contract accepted", nil
		}

		// Designer can search for information about targets
		searcherHandler, err := fp.GetSubtaskSearcherHandler(ctx, &taskID, nil)
		if err == nil {
			searchDef := tools.GetRegistryDefinitions()[tools.SearchToolName]
			if searchDef.Name != "" {
				defs = append(defs, searchDef)
				handlers[tools.SearchToolName] = searcherHandler
			}
		}

		// Designer can retrieve memories from past tasks
		memoristHandler, err := fp.GetMemoristHandler(ctx, &taskID, nil)
		if err == nil {
			memDef := tools.GetRegistryDefinitions()[tools.MemoristToolName]
			if memDef.Name != "" {
				defs = append(defs, memDef)
				handlers[tools.MemoristToolName] = memoristHandler
			}
		}
	}

	if nodeRole == "supervisor" {
		// Supervisor delegates to all agent roles via route_to_* tools
		routeMappings := map[string]orchestrator.AgentRole{
			// Note: route_to_designer is intentionally excluded — topology is one-way
			// (designer -> planner -> supervisor -> agents). Re-enable if graph changes.
			tools.RouteToPlannerToolName:    orchestrator.AgentRolePlanner,
			tools.RouteToBuilderToolName:    orchestrator.AgentRoleBuilder,
			tools.RouteToGeneratorToolName:  orchestrator.AgentRoleGenerator,
			tools.RouteToIntegratorToolName: orchestrator.AgentRoleIntegrator,
			tools.RouteToTesterToolName:     orchestrator.AgentRoleTester,
			tools.RouteToPentesterToolName:  orchestrator.AgentRolePentester,
			tools.RouteToReviewerToolName:   orchestrator.AgentRoleReviewer,
			tools.RouteToReporterToolName:   orchestrator.AgentRoleReporter,
			tools.RouteToResearcherToolName: orchestrator.AgentRoleResearcher,
		}

		for toolName, role := range routeMappings {
			def, ok := tools.GetRegistryDefinitions()[toolName]
			if !ok {
				continue
			}
			defs = append(defs, def)
			barriers = append(barriers, toolName)
			capturedRole := role
			handlers[toolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
				decision.Action = orchestrator.SupervisorActionDelegate
				decision.AgentRole = capturedRole
				decision.Payload = append(json.RawMessage(nil), args...)
				// Extract todo_id from args if present
				var routeArgs struct {
					TodoID string `json:"todo_id"`
				}
				if json.Unmarshal(args, &routeArgs) == nil && routeArgs.TodoID != "" {
					decision.TodoID = routeArgs.TodoID
				}
				return fmt.Sprintf("delegated to %s; waiting for execution", capturedRole), nil
			}
		}

		// Auth request tool
		authDef := tools.GetRegistryDefinitions()[tools.AuthRequestToolName]
		if authDef.Name != "" {
			defs = append(defs, authDef)
			barriers = append(barriers, tools.AuthRequestToolName)
			handlers[tools.AuthRequestToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
				decision.Action = orchestrator.SupervisorActionAuthRequired
				decision.Message = string(args)
				return "authorization requested", nil
			}
		}
	}

	// Planner node uses todo_list and todo_patch as barrier tools
	if nodeRole == "planner" {
		todoListDef := tools.GetRegistryDefinitions()[tools.TodoListToolName]
		todoPatchDef := tools.GetRegistryDefinitions()[tools.TodoPatchToolName]
		if todoListDef.Name != "" {
			defs = append(defs, todoListDef)
			barriers = append(barriers, tools.TodoListToolName)
			handlers[tools.TodoListToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
				decision.Action = orchestrator.SupervisorActionPlanReady
				decision.Result = string(args)
				return "todo plan accepted", nil
			}
		}
		if todoPatchDef.Name != "" {
			defs = append(defs, todoPatchDef)
			barriers = append(barriers, tools.TodoPatchToolName)
			handlers[tools.TodoPatchToolName] = func(ctx context.Context, name string, args json.RawMessage) (string, error) {
				decision.Action = orchestrator.SupervisorActionPlanReady
				decision.Result = string(args)
				return "todo patch accepted", nil
			}
		}
	}

	return defs, handlers, barriers, nil
}

