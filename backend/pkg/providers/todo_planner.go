package providers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"pentagi/pkg/cast"
	"pentagi/pkg/csum"
	"pentagi/pkg/database"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/observability/langfuse"
	"pentagi/pkg/providers/pconfig"
	"pentagi/pkg/templates"
	"pentagi/pkg/tools"

	"github.com/sirupsen/logrus"
	"github.com/vxcontrol/langchaingo/llms"
)

type dbAccessor interface {
	DB() database.DBTX
}

type plannerTodoView struct {
	TodoID               string
	Title                string
	OwnerAgent           string
	DependsOn            []string
	NeedEnv              bool
	NeedCode             bool
	RiskLevel            string
	AuthRequired         bool
	Inputs               string
	SuccessCriteria      string
	EvidenceRequirements []string
	Status               string
	Result               string
}

func (fp *flowProvider) GenerateTodoPlan(ctx context.Context, taskID int64) ([]tools.TodoItem, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.GenerateTodoPlan")
	defer span.End()

	return fp.planTodos(ctx, taskID, "generate", nil)
}

func (fp *flowProvider) RefineTodoPlan(ctx context.Context, taskID int64) ([]tools.TodoItem, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.RefineTodoPlan")
	defer span.End()

	ma, err := fp.multiAgentQueries()
	if err != nil {
		return nil, err
	}

	todos, err := ma.GetTodosByTaskID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get todos for task %d: %w", taskID, err)
	}

	return fp.planTodos(ctx, taskID, "refine", todos)
}

func (fp *flowProvider) planTodos(
	ctx context.Context,
	taskID int64,
	mode string,
	existingTodos []database.Todo,
) ([]tools.TodoItem, error) {
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"task_id": taskID,
		"mode":    mode,
	})

	tasksInfo, err := fp.getTasksInfo(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks info: %w", err)
	}

	ma, err := fp.multiAgentQueries()
	if err != nil {
		return nil, err
	}

	ext, err := ma.GetTaskExtension(ctx, taskID)
	if err != nil {
		logger.WithError(err).Warn("failed to load task extension for planner context")
		ext = &database.TaskExt{}
	}

	split := splitPlannerTodos(existingTodos)
	userContext := map[string]any{
		"Mode":           mode,
		"Task":           tasksInfo.Task,
		"Tasks":          tasksInfo.Tasks,
		"ScopeContract":  rawJSONToString(ext.ScopeContract),
		"SharedState":    rawJSONToString(ext.SharedState),
		"PlannedTodos":   split.Planned,
		"CompletedTodos": split.Completed,
		"BlockedTodos":   split.Blocked,
		"FailedTodos":    split.Failed,
	}
	systemContext := map[string]any{
		"Mode":                    mode,
		"TodoListToolName":        tools.TodoListToolName,
		"TodoPatchToolName":       tools.TodoPatchToolName,
		"SearchToolName":          tools.SearchToolName,
		"TerminalToolName":        tools.TerminalToolName,
		"FileToolName":            tools.FileToolName,
		"BrowserToolName":         tools.BrowserToolName,
		"SummarizationToolName":   cast.SummarizationToolName,
		"SummarizedContentPrefix": strings.ReplaceAll(csum.SummarizedContentPrefix, "\n", "\\n"),
		"DockerImage":             fp.image,
		"Lang":                    fp.language,
		"CurrentTime":             getCurrentTime(),
		"N":                       TasksNumberLimit,
		"ToolPlaceholder":         ToolPlaceholder,
	}

	ctx, observation := obs.Observer.NewObservation(ctx)
	evaluator := observation.Evaluator(
		langfuse.WithEvaluatorName("todo planner"),
		langfuse.WithEvaluatorInput(userContext),
		langfuse.WithEvaluatorMetadata(langfuse.Metadata{
			"user_context":   userContext,
			"system_context": systemContext,
		}),
	)
	ctx, _ = evaluator.Observation(ctx)

	systemTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypePlanner, systemContext)
	if err != nil {
		return nil, wrapErrorEndEvaluatorSpan(ctx, evaluator, "failed to render planner system template", err)
	}
	userTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeQuestionPlanner, userContext)
	if err != nil {
		return nil, wrapErrorEndEvaluatorSpan(ctx, evaluator, "failed to render planner question template", err)
	}

	todos, err := fp.performTodoPlanner(ctx, taskID, mode, existingTodos, systemTmpl, userTmpl, tasksInfo.Task.Input)
	if err != nil {
		return nil, wrapErrorEndEvaluatorSpan(ctx, evaluator, "failed to perform todo planner", err)
	}

	evaluator.End(
		langfuse.WithEvaluatorStatus("success"),
		langfuse.WithEvaluatorOutput(todos),
	)

	return todos, nil
}

func (fp *flowProvider) performTodoPlanner(
	ctx context.Context,
	taskID int64,
	mode string,
	existingTodos []database.Todo,
	systemTmpl, userTmpl, input string,
) ([]tools.TodoItem, error) {
	var (
		todoList  tools.TodoListAction
		todoPatch tools.TodoPatchAction
	)

	optAgentType := pconfig.OptionsTypeGenerator
	if mode == "refine" {
		optAgentType = pconfig.OptionsTypeRefiner
	}

	chain := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, systemTmpl),
		llms.TextParts(llms.ChatMessageTypeHuman, userTmpl),
	}

	memorist, err := fp.GetMemoristHandler(ctx, &taskID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get memorist handler: %w", err)
	}

	searcher, err := fp.GetTaskSearcherHandler(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get searcher handler: %w", err)
	}

	ctx = tools.PutAgentContext(ctx, database.MsgchainTypePlanner)
	cfg := tools.PlannerExecutorConfig{
		TaskID:   taskID,
		Memorist: memorist,
		Searcher: searcher,
		TodoList: func(ctx context.Context, name string, args json.RawMessage) (string, error) {
			if err := json.Unmarshal(args, &todoList); err != nil {
				return "", fmt.Errorf("failed to unmarshal todo list: %w", err)
			}
			return "todo list successfully processed", nil
		},
		TodoPatch: func(ctx context.Context, name string, args json.RawMessage) (string, error) {
			if err := json.Unmarshal(args, &todoPatch); err != nil {
				return "", fmt.Errorf("failed to unmarshal todo patch: %w", err)
			}
			if err := ValidateTodoPatch(todoPatch); err != nil {
				return "", fmt.Errorf("invalid todo patch: %w", err)
			}
			return "todo patch successfully processed", nil
		},
	}

	executor, err := fp.executor.GetPlannerExecutor(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get planner executor: %w", err)
	}

	chainBlob, err := json.Marshal(chain)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal planner chain: %w", err)
	}

	startTime := time.Now()
	msgChain, err := fp.db.CreateMsgChain(ctx, database.CreateMsgChainParams{
		Type:            database.MsgchainTypePlanner,
		Model:           fp.Model(optAgentType),
		ModelProvider:   string(fp.Type()),
		Chain:           chainBlob,
		FlowID:          fp.flowID,
		TaskID:          database.Int64ToNullInt64(&taskID),
		DurationSeconds: time.Since(startTime).Seconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create planner msg chain: %w", err)
	}

	if err := fp.performAgentChain(ctx, optAgentType, msgChain.ID, &taskID, nil, chain, executor, fp.summarizer); err != nil {
		return nil, fmt.Errorf("failed to get planner result: %w", err)
	}

	var todos []tools.TodoItem
	switch mode {
	case "generate":
		todos = normalizeTodoItems(todoList.Todos)
	case "refine":
		mutableTodos, completedTodos := splitMutableTodoItems(existingTodos)
		planned, err := applyTodoOperations(mutableTodos, todoPatch, logrus.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		todos = append(completedTodos, planned...)
	default:
		return nil, fmt.Errorf("unsupported planner mode %q", mode)
	}

	if agentCtx, ok := tools.GetAgentContext(ctx); ok {
		fp.putAgentLog(
			ctx,
			agentCtx.ParentAgentType,
			agentCtx.CurrentAgentType,
			input,
			fp.todosToMarkdown(todos),
			&taskID,
			nil,
		)
	}

	return todos, nil
}

func (fp *flowProvider) multiAgentQueries() (*database.MultiAgentQueries, error) {
	accessor, ok := fp.db.(dbAccessor)
	if !ok {
		return nil, fmt.Errorf("database querier does not expose raw DB access")
	}
	return database.NewMultiAgentQueries(accessor.DB()), nil
}

func splitMutableTodoItems(todos []database.Todo) ([]tools.TodoItem, []tools.TodoItem) {
	mutable := make([]tools.TodoItem, 0, len(todos))
	completed := make([]tools.TodoItem, 0)
	for _, todo := range todos {
		item := dbTodoToTool(todo)
		switch todo.Status {
		case "completed", "done":
			completed = append(completed, item)
		default:
			mutable = append(mutable, item)
		}
	}
	return mutable, completed
}

type plannerTodoSplit struct {
	Planned   []plannerTodoView
	Completed []plannerTodoView
	Blocked   []plannerTodoView
	Failed    []plannerTodoView
}

func splitPlannerTodos(todos []database.Todo) plannerTodoSplit {
	var split plannerTodoSplit
	for _, todo := range todos {
		view := dbTodoToView(todo)
		switch todo.Status {
		case "completed", "done":
			split.Completed = append(split.Completed, view)
		case "failed":
			split.Failed = append(split.Failed, view)
		case "blocked":
			split.Blocked = append(split.Blocked, view)
		default:
			split.Planned = append(split.Planned, view)
		}
	}
	return split
}

func dbTodoToTool(todo database.Todo) tools.TodoItem {
	view := dbTodoToView(todo)
	return tools.TodoItem{
		TodoID:               view.TodoID,
		Title:                view.Title,
		OwnerAgent:           view.OwnerAgent,
		DependsOn:            view.DependsOn,
		NeedEnv:              view.NeedEnv,
		NeedCode:             view.NeedCode,
		RiskLevel:            view.RiskLevel,
		AuthRequired:         view.AuthRequired,
		Inputs:               view.Inputs,
		SuccessCriteria:      view.SuccessCriteria,
		EvidenceRequirements: view.EvidenceRequirements,
		Status:               view.Status,
	}
}

func dbTodoToView(todo database.Todo) plannerTodoView {
	return plannerTodoView{
		TodoID:               todo.TodoID,
		Title:                todo.Title,
		OwnerAgent:           todo.OwnerAgent,
		DependsOn:            decodeStringList(todo.DependsOn),
		NeedEnv:              todo.NeedEnv,
		NeedCode:             todo.NeedCode,
		RiskLevel:            todo.RiskLevel,
		AuthRequired:         todo.AuthRequired,
		Inputs:               nullString(todo.Inputs),
		SuccessCriteria:      nullString(todo.SuccessCriteria),
		EvidenceRequirements: decodeStringList(todo.EvidenceRequirements),
		Status:               todo.Status,
		Result:               nullString(todo.Result),
	}
}

func decodeStringList(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	return values
}

func rawJSONToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	return string(raw)
}

func nullString(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}

func (fp *flowProvider) todosToMarkdown(todos []tools.TodoItem) string {
	var buffer strings.Builder
	for idx, todo := range todos {
		buffer.WriteString(fmt.Sprintf("# Todo %d: %s\n\n", idx+1, todo.Title))
		buffer.WriteString(fmt.Sprintf("- id: %s\n", todo.TodoID))
		buffer.WriteString(fmt.Sprintf("- owner_agent: %s\n", todo.OwnerAgent))
		buffer.WriteString(fmt.Sprintf("- status: %s\n", todo.Status))
		buffer.WriteString(fmt.Sprintf("- risk_level: %s\n", todo.RiskLevel))
		buffer.WriteString(fmt.Sprintf("- auth_required: %t\n", todo.AuthRequired))
		if todo.Inputs != "" {
			buffer.WriteString(fmt.Sprintf("- inputs: %s\n", todo.Inputs))
		}
		if todo.SuccessCriteria != "" {
			buffer.WriteString(fmt.Sprintf("- success_criteria: %s\n", todo.SuccessCriteria))
		}
		buffer.WriteString("\n")
	}
	return buffer.String()
}
