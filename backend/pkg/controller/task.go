package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"pentagi/pkg/database"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/orchestrator"
	"pentagi/pkg/providers"
	"pentagi/pkg/tools"
)

type FlowUpdater interface {
	SetStatus(ctx context.Context, status database.FlowStatus) error
}

type TaskWorker interface {
	GetTaskID() int64
	GetFlowID() int64
	GetUserID() int64
	GetTitle() string
	IsCompleted() bool
	IsWaiting() bool
	GetStatus(ctx context.Context) (database.TaskStatus, error)
	SetStatus(ctx context.Context, status database.TaskStatus) error
	GetResult(ctx context.Context) (string, error)
	SetResult(ctx context.Context, result string) error
	PutInput(ctx context.Context, input string) error
	GenerateSubtasks(ctx context.Context) error
	RefineSubtasks(ctx context.Context) error
	SelectNextSubtask(ctx context.Context) (SubtaskWorker, error)
	GetSubtask(ctx context.Context, subtaskID int64) (SubtaskWorker, error)
	ReportTaskResult(ctx context.Context) error
	Fail(ctx context.Context, result string) error
	Run(ctx context.Context) error
	Finish(ctx context.Context) error
	// Multi-agent migration: new methods
	DesignerStep(ctx context.Context) (*orchestrator.SupervisorDecision, error)
	PlannerStep(ctx context.Context) (*orchestrator.SupervisorDecision, error)
	SupervisorStep(ctx context.Context) (*orchestrator.SupervisorDecision, error)
	GenerateTodoPlan(ctx context.Context) ([]database.Todo, error)
	RefineTodoPlan(ctx context.Context) ([]database.Todo, error)
	AgentExecute(ctx context.Context, agentRole, todoID string, payload json.RawMessage) (*orchestrator.AgentExecutionResult, error)
	StoreArtifact(ctx context.Context, artifactID, name, artifactType, content string) error
	StoreAuthRequest(ctx context.Context, todoID, action, riskLevel, justification string) error
	ResolveAuthRequest(ctx context.Context, authID, status, response string) error
	StoreFinding(ctx context.Context, todoID, findingType, severity, title, description, rawOutput string) error
	RejectTask(ctx context.Context, result string) error
	CompleteTask(ctx context.Context) error
	UpdateSharedState(ctx context.Context, activeNode, activeTodoID string, statusCode *int, updates map[string]interface{}) error
}

type taskWorker struct {
	mx        *sync.RWMutex
	stc       SubtaskController
	taskCtx   *TaskContext
	updater   FlowUpdater
	completed bool
	waiting   bool
}

func NewTaskWorker(
	ctx context.Context,
	flowCtx *FlowContext,
	input string,
	updater FlowUpdater,
) (TaskWorker, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "controller.NewTaskWorker")
	defer span.End()

	ctx = tools.PutAgentContext(ctx, database.MsgchainTypePrimaryAgent)

	title, err := flowCtx.Provider.GetTaskTitle(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get task title: %w", err)
	}

	task, err := flowCtx.DB.CreateTask(ctx, database.CreateTaskParams{
		Status: database.TaskStatusCreated,
		Title:  title,
		Input:  input,
		FlowID: flowCtx.FlowID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create task in DB: %w", err)
	}

	flowCtx.Publisher.TaskCreated(ctx, task, []database.Subtask{})

	taskCtx := &TaskContext{
		FlowContext: *flowCtx,
		TaskID:      task.ID,
		TaskTitle:   title,
		TaskInput:   input,
	}
	stc := NewSubtaskController(taskCtx)

	_, err = taskCtx.MsgLog.PutTaskMsg(
		ctx,
		database.MsglogTypeInput,
		taskCtx.TaskID,
		"", // thinking is empty because this is input
		input,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to put input for task %d: %w", taskCtx.TaskID, err)
	}

	if flowCtx.Orchestrator == nil {
		err = stc.GenerateSubtasks(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to generate subtasks: %w", err)
		}
	}

	subtasks, err := flowCtx.DB.GetTaskSubtasks(ctx, task.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get subtasks for task %d: %w", task.ID, err)
	}

	flowCtx.Publisher.TaskUpdated(ctx, task, subtasks)

	return &taskWorker{
		mx:        &sync.RWMutex{},
		stc:       stc,
		taskCtx:   taskCtx,
		updater:   updater,
		completed: false,
		waiting:   false,
	}, nil
}

func LoadTaskWorker(
	ctx context.Context,
	task database.Task,
	flowCtx *FlowContext,
	updater FlowUpdater,
) (TaskWorker, error) {
	ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "controller.LoadTaskWorker")
	defer span.End()

	ctx = tools.PutAgentContext(ctx, database.MsgchainTypePrimaryAgent)
	taskCtx := &TaskContext{
		FlowContext: *flowCtx,
		TaskID:      task.ID,
		TaskTitle:   task.Title,
		TaskInput:   task.Input,
	}

	stc := NewSubtaskController(taskCtx)
	var completed, waiting bool
	switch task.Status {
	case database.TaskStatusFinished, database.TaskStatusFailed:
		completed = true
	case database.TaskStatusWaiting:
		waiting = true
	case database.TaskStatusRunning:
	case database.TaskStatusCreated:
		return nil, fmt.Errorf("task %d has created yet: loading aborted: %w", task.ID, ErrNothingToLoad)
	}

	tw := &taskWorker{
		mx:        &sync.RWMutex{},
		stc:       stc,
		taskCtx:   taskCtx,
		updater:   updater,
		completed: completed,
		waiting:   waiting,
	}

	if err := tw.stc.LoadSubtasks(ctx, task.ID, tw); err != nil {
		return nil, fmt.Errorf("failed to load subtasks for task %d: %w", task.ID, err)
	}

	return tw, nil
}

func (tw *taskWorker) GetTaskID() int64 {
	return tw.taskCtx.TaskID
}

func (tw *taskWorker) GetFlowID() int64 {
	return tw.taskCtx.FlowID
}

func (tw *taskWorker) GetUserID() int64 {
	return tw.taskCtx.UserID
}

func (tw *taskWorker) GetTitle() string {
	return tw.taskCtx.TaskTitle
}

func (tw *taskWorker) IsCompleted() bool {
	tw.mx.RLock()
	defer tw.mx.RUnlock()

	return tw.completed
}

func (tw *taskWorker) IsWaiting() bool {
	tw.mx.RLock()
	defer tw.mx.RUnlock()

	return tw.waiting
}

func (tw *taskWorker) GetStatus(ctx context.Context) (database.TaskStatus, error) {
	task, err := tw.taskCtx.DB.GetTask(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return database.TaskStatusFailed, err
	}

	return task.Status, nil
}

// this function is exclusively change task internal properties "completed" and "waiting"
func (tw *taskWorker) SetStatus(ctx context.Context, status database.TaskStatus) error {
	task, err := tw.taskCtx.DB.UpdateTaskStatus(ctx, database.UpdateTaskStatusParams{
		Status: status,
		ID:     tw.taskCtx.TaskID,
	})
	if err != nil {
		return fmt.Errorf("failed to set task %d status: %w", tw.taskCtx.TaskID, err)
	}

	subtasks, err := tw.taskCtx.DB.GetTaskSubtasks(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task %d subtasks: %w", tw.taskCtx.TaskID, err)
	}

	tw.taskCtx.Publisher.TaskUpdated(ctx, task, subtasks)

	tw.mx.Lock()
	defer tw.mx.Unlock()

	switch status {
	case database.TaskStatusRunning:
		tw.completed = false
		tw.waiting = false
		err = tw.updater.SetStatus(ctx, database.FlowStatusRunning)
	case database.TaskStatusWaiting:
		tw.completed = false
		tw.waiting = true
		err = tw.updater.SetStatus(ctx, database.FlowStatusWaiting)
	case database.TaskStatusFinished, database.TaskStatusFailed:
		tw.completed = true
		tw.waiting = false
		// the last task was done, set flow status to Waiting new user input
		err = tw.updater.SetStatus(ctx, database.FlowStatusWaiting)
	default:
		// status Created is not possible to set by this call
		return fmt.Errorf("unsupported task status: %s", status)
	}
	if err != nil {
		return fmt.Errorf("failed to set flow status in back propagation: %w", err)
	}

	return nil
}

func (tw *taskWorker) GetResult(ctx context.Context) (string, error) {
	task, err := tw.taskCtx.DB.GetTask(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return "", err
	}

	return task.Result, nil
}

func (tw *taskWorker) SetResult(ctx context.Context, result string) error {
	_, err := tw.taskCtx.DB.UpdateTaskResult(ctx, database.UpdateTaskResultParams{
		Result: result,
		ID:     tw.taskCtx.TaskID,
	})
	if err != nil {
		return fmt.Errorf("failed to set task %d result: %w", tw.taskCtx.TaskID, err)
	}

	return nil
}

func (tw *taskWorker) PutInput(ctx context.Context, input string) error {
	if !tw.IsWaiting() {
		return fmt.Errorf("task is not waiting")
	}

	for _, st := range tw.stc.ListSubtasks(ctx) {
		if !st.IsCompleted() && st.IsWaiting() {
			if err := st.PutInput(ctx, input); err != nil {
				return fmt.Errorf("failed to put input to subtask %d: %w", st.GetSubtaskID(), err)
			} else {
				break
			}
		}
	}

	return nil
}

func (tw *taskWorker) GenerateSubtasks(ctx context.Context) error {
	if err := tw.stc.GenerateSubtasks(ctx); err != nil {
		return err
	}

	task, err := tw.taskCtx.DB.GetTask(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task %d: %w", tw.taskCtx.TaskID, err)
	}

	subtasks, err := tw.taskCtx.DB.GetTaskSubtasks(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get subtasks for task %d: %w", tw.taskCtx.TaskID, err)
	}

	tw.taskCtx.Publisher.TaskUpdated(ctx, task, subtasks)

	return nil
}

func (tw *taskWorker) RefineSubtasks(ctx context.Context) error {
	if err := tw.stc.RefineSubtasks(ctx); err != nil {
		return err
	}

	task, err := tw.taskCtx.DB.GetTask(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task %d: %w", tw.taskCtx.TaskID, err)
	}

	subtasks, err := tw.taskCtx.DB.GetTaskSubtasks(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get subtasks for task %d: %w", tw.taskCtx.TaskID, err)
	}

	tw.taskCtx.Publisher.TaskUpdated(ctx, task, subtasks)

	return nil
}

func (tw *taskWorker) SelectNextSubtask(ctx context.Context) (SubtaskWorker, error) {
	return tw.stc.PopSubtask(ctx, tw)
}

func (tw *taskWorker) GetSubtask(ctx context.Context, subtaskID int64) (SubtaskWorker, error) {
	return tw.stc.GetSubtask(ctx, subtaskID)
}

func (tw *taskWorker) ReportTaskResult(ctx context.Context) error {
	jobResult, err := tw.taskCtx.Provider.GetTaskResult(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to get task %d result: %w", tw.taskCtx.TaskID, err)
	}

	var taskStatus database.TaskStatus
	if jobResult.Success {
		taskStatus = database.TaskStatusFinished
	} else {
		taskStatus = database.TaskStatusFailed
	}

	return tw.finalizeTask(ctx, taskStatus, jobResult.Result)
}

func (tw *taskWorker) Fail(ctx context.Context, result string) error {
	if result == "" {
		result = "task failed"
	}

	return tw.finalizeTask(ctx, database.TaskStatusFailed, result)
}

func (tw *taskWorker) finalizeTask(ctx context.Context, status database.TaskStatus, result string) error {
	if err := tw.SetResult(ctx, result); err != nil {
		return err
	}

	if err := tw.SetStatus(ctx, status); err != nil {
		return err
	}

	format := database.MsglogResultFormatMarkdown
	_, err := tw.taskCtx.MsgLog.PutTaskMsgResult(
		ctx,
		database.MsglogTypeReport,
		tw.taskCtx.TaskID,
		"",
		tw.taskCtx.TaskTitle,
		result,
		format,
	)
	if err != nil {
		return fmt.Errorf("failed to put report for task %d: %w", tw.taskCtx.TaskID, err)
	}

	return nil
}

func (tw *taskWorker) Run(ctx context.Context) error {
	ctx = tools.PutAgentContext(ctx, database.MsgchainTypePrimaryAgent)

	if tw.taskCtx.Orchestrator != nil {
		return tw.runWithOrchestrator(ctx)
	}

	for len(tw.stc.ListSubtasks(ctx)) < providers.TasksNumberLimit+3 {
		st, err := tw.stc.PopSubtask(ctx, tw)
		if err != nil {
			return err
		}

		// empty queue for subtasks means that task is done
		if st == nil {
			break
		}

		if err := st.Run(ctx); err != nil {
			return err
		}

		// pass through if task is waiting from back status propagation
		if tw.IsWaiting() {
			return nil
		} // otherwise subtask is done

		if err := tw.stc.RefineSubtasks(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				ctx = context.Background()
			}
			_ = tw.SetStatus(ctx, database.TaskStatusWaiting)
			return fmt.Errorf("failed to refine subtasks list for the task %d: %w", tw.taskCtx.TaskID, err)
		}
	}

	return tw.ReportTaskResult(ctx)
}

func (tw *taskWorker) Finish(ctx context.Context) error {
	if tw.IsCompleted() {
		return fmt.Errorf("task has already completed")
	}

	for _, st := range tw.stc.ListSubtasks(ctx) {
		if !st.IsCompleted() {
			if err := st.Finish(ctx); err != nil {
				return err
			}
		}
	}

	if err := tw.SetStatus(ctx, database.TaskStatusFinished); err != nil {
		return err
	}

	return nil
}

func (tw *taskWorker) runWithOrchestrator(ctx context.Context) error {
	status, err := tw.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get task %d status before orchestration: %w", tw.taskCtx.TaskID, err)
	}

	switch status {
	case database.TaskStatusCreated:
		err = tw.taskCtx.Orchestrator.StartTask(ctx, tw.taskCtx.FlowID, tw.taskCtx.TaskID)
	default:
		err = tw.taskCtx.Orchestrator.ResumeTask(ctx, tw.taskCtx.FlowID, tw.taskCtx.TaskID)
	}
	if err != nil {
		return fmt.Errorf("failed to orchestrate task %d via langgraph: %w", tw.taskCtx.TaskID, err)
	}

	return nil
}

// ========================================
// Multi-agent migration: new method implementations
// ========================================

func (tw *taskWorker) DesignerStep(ctx context.Context) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "designer")
	if err != nil {
		return nil, fmt.Errorf("failed to execute designer step: %w", err)
	}
	return decision, nil
}

func (tw *taskWorker) PlannerStep(ctx context.Context) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "planner")
	if err != nil {
		return nil, fmt.Errorf("failed to execute planner step: %w", err)
	}
	return decision, nil
}

func (tw *taskWorker) SupervisorStep(ctx context.Context) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "supervisor")
	if err != nil {
		return nil, fmt.Errorf("failed to execute supervisor step: %w", err)
	}
	return decision, nil
}

func (tw *taskWorker) GenerateTodoPlan(ctx context.Context) ([]database.Todo, error) {
	plan, err := tw.taskCtx.Provider.GenerateTodoPlan(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate todo plan: %w", err)
	}

	return tw.replaceTodoPlan(ctx, plan)
}

func (tw *taskWorker) RefineTodoPlan(ctx context.Context) ([]database.Todo, error) {
	plan, err := tw.taskCtx.Provider.RefineTodoPlan(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to refine todo plan: %w", err)
	}

	return tw.replaceTodoPlan(ctx, plan)
}

func (tw *taskWorker) AgentExecute(ctx context.Context, agentRole, todoID string, payload json.RawMessage) (*orchestrator.AgentExecutionResult, error) {
	result, err := tw.taskCtx.Provider.ExecuteDelegatedAgent(ctx, tw.taskCtx.TaskID, 0, agentRole, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to execute agent %s: %w", agentRole, err)
	}
	return result, nil
}

func (tw *taskWorker) StoreArtifact(ctx context.Context, artifactID, name, artifactType, content string) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	return ma.UpsertArtifact(ctx, &database.Artifact{
		TaskID:       tw.taskCtx.TaskID,
		ArtifactID:   artifactID,
		Name:         name,
		ArtifactType: artifactType,
		Text:         sqlPtrString(content),
	})
}

func (tw *taskWorker) StoreAuthRequest(ctx context.Context, todoID, action, riskLevel, justification string) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	return ma.InsertAuthRequest(ctx, &database.AuthRequest{
		TaskID:        tw.taskCtx.TaskID,
		TodoID:        sqlPtrString(todoID),
		Action:        action,
		RiskLevel:     riskLevel,
		Justification: justification,
		Status:        "pending",
	})
}

func (tw *taskWorker) ResolveAuthRequest(ctx context.Context, authID, status, response string) error {
	return fmt.Errorf("not implemented: resolve auth request")
}

func (tw *taskWorker) StoreFinding(ctx context.Context, todoID, findingType, severity, title, description, rawOutput string) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	return ma.InsertFinding(ctx, &database.Finding{
		TaskID:      tw.taskCtx.TaskID,
		TodoID:      sqlPtrString(todoID),
		FindingType: sqlPtrString(findingType),
		Severity:    sqlPtrString(severity),
		Title:       title,
		Description: sqlPtrString(description),
		RawOutput:   sqlPtrString(rawOutput),
	})
}

func (tw *taskWorker) RejectTask(ctx context.Context, result string) error {
	return tw.finalizeTask(ctx, database.TaskStatusFailed, "REJECTED: "+result)
}

func (tw *taskWorker) CompleteTask(ctx context.Context) error {
	return tw.ReportTaskResult(ctx)
}

func (tw *taskWorker) UpdateSharedState(ctx context.Context, activeNode, activeTodoID string, statusCode *int, updates map[string]interface{}) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	ext := &database.TaskExt{
		ActiveNode:   sqlPtrString(activeNode),
		ActiveTodoID: sqlPtrString(activeTodoID),
	}
	if statusCode != nil {
		ext.TaskStatusCode = *statusCode
	}
	if updates != nil {
		sharedState, _ := json.Marshal(updates)
		ext.SharedState = sharedState
	}
	return ma.UpdateTaskExtension(ctx, tw.taskCtx.TaskID, ext)
}

func (tw *taskWorker) replaceTodoPlan(ctx context.Context, plan []tools.TodoItem) ([]database.Todo, error) {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	existing, err := ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing todos for task %d: %w", tw.taskCtx.TaskID, err)
	}

	existingByID := make(map[string]database.Todo, len(existing))
	for _, todo := range existing {
		existingByID[todo.TodoID] = todo
	}

	todos, err := todoItemsToDB(tw.taskCtx.TaskID, plan, existingByID)
	if err != nil {
		return nil, err
	}

	if err := ma.ReplaceTodosByTaskID(ctx, tw.taskCtx.TaskID, todos); err != nil {
		return nil, fmt.Errorf("failed to replace todos for task %d: %w", tw.taskCtx.TaskID, err)
	}

	return ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
}

func todoItemsToDB(taskID int64, plan []tools.TodoItem, existing map[string]database.Todo) ([]database.Todo, error) {
	todos := make([]database.Todo, 0, len(plan))
	for i, item := range plan {
		if item.TodoID == "" {
			item.TodoID = fmt.Sprintf("todo_%03d", i+1)
		}
		if item.OwnerAgent == "" {
			item.OwnerAgent = "pentester"
		}
		if item.RiskLevel == "" {
			item.RiskLevel = "low"
		}
		if item.Status == "" {
			item.Status = "pending"
		}

		dependsOn, err := json.Marshal(item.DependsOn)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal depends_on for todo %s: %w", item.TodoID, err)
		}
		evidenceRequirements, err := json.Marshal(item.EvidenceRequirements)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal evidence requirements for todo %s: %w", item.TodoID, err)
		}

		todo := database.Todo{
			TaskID:               taskID,
			TodoID:               item.TodoID,
			Title:                item.Title,
			OwnerAgent:           item.OwnerAgent,
			DependsOn:            dependsOn,
			NeedEnv:              item.NeedEnv,
			NeedCode:             item.NeedCode,
			RiskLevel:            item.RiskLevel,
			AuthRequired:         item.AuthRequired,
			Inputs:               sqlPtrString(item.Inputs),
			SuccessCriteria:      sqlPtrString(item.SuccessCriteria),
			EvidenceRequirements: evidenceRequirements,
			Data:                 json.RawMessage(`{}`),
			Status:               item.Status,
		}

		if prev, ok := existing[item.TodoID]; ok {
			todo.Result = prev.Result
			todo.TodoStatusCode = prev.TodoStatusCode
			if item.Status == "" {
				todo.Status = prev.Status
			}
			if len(prev.Data) > 0 {
				todo.Data = prev.Data
			}
		}

		todos = append(todos, todo)
	}
	return todos, nil
}

// sqlPtrString converts a string to sql.NullString
func sqlPtrString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}
