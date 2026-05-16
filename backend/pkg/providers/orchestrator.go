package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"pentagi/pkg/database"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/orchestrator"
	"pentagi/pkg/providers/pconfig"
	"pentagi/pkg/tools"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/langchaingo/llms"
)

func toolNameToAgentType(toolName string) (string, error) {
	switch toolName {
	case tools.CoderToolName:
		return "coder", nil
	case tools.PentesterToolName:
		return "pentester", nil
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
	case "coder":
		return tools.CoderToolName, nil
	case "pentester":
		return tools.PentesterToolName, nil
	case "searcher":
		return tools.SearchToolName, nil
	case "installer":
		return tools.MaintenanceToolName, nil
	case "memorist":
		return tools.MemoristToolName, nil
	case "adviser":
		return tools.AdviceToolName, nil
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
		Memorist:   buildAgentHandler("memorist"),
		Pentester:  buildAgentHandler("pentester"),
		Searcher:   buildAgentHandler("searcher"),
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

	var handler tools.ExecutorHandler
	switch agentType {
	case "coder":
		handler, err = fp.GetCoderHandler(ctx, &taskID, &subtaskID)
	case "pentester":
		handler, err = fp.GetPentesterHandler(ctx, &taskID, &subtaskID)
	case "searcher":
		handler, err = fp.GetSubtaskSearcherHandler(ctx, &taskID, &subtaskID)
	case "installer":
		handler, err = fp.GetInstallerHandler(ctx, &taskID, &subtaskID)
	case "memorist":
		handler, err = fp.GetMemoristHandler(ctx, &taskID, &subtaskID)
	case "adviser":
		handler, err = fp.GetAskAdviceHandler(ctx, &taskID, &subtaskID)
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
