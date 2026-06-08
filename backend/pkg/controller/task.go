package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
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
	DesignerStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error)
	PlannerStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error)
	SupervisorStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error)
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
	mx           *sync.RWMutex
	stc          SubtaskController
	taskCtx      *TaskContext
	updater      FlowUpdater
	completed    bool
	waiting      bool
	pendingInput string
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

	if tw.taskCtx.Orchestrator != nil {
		tw.mx.Lock()
		tw.pendingInput = input
		tw.mx.Unlock()
		return nil
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

	var snapshot *orchestrator.TaskSnapshot
	switch status {
	case database.TaskStatusCreated:
		snapshot, err = tw.taskCtx.Orchestrator.StartTask(ctx, tw.taskCtx.FlowID, tw.taskCtx.TaskID)
	default:
		// task in WAITING state → pass user input to resume
		tw.mx.Lock()
		userInput := tw.pendingInput
		tw.mx.Unlock()
		snapshot, err = tw.taskCtx.Orchestrator.ResumeTask(ctx, tw.taskCtx.FlowID, tw.taskCtx.TaskID, userInput)
		if err == nil {
			// Only clear pendingInput after successful resume to prevent data loss on transient errors
			tw.mx.Lock()
			tw.pendingInput = ""
			tw.mx.Unlock()
		}
	}
	if err != nil {
		return fmt.Errorf("failed to orchestrate task %d via langgraph: %w", tw.taskCtx.TaskID, err)
	}

	if snapshot == nil {
		return nil
	}

	if snapshot.HasInterrupt {
		if snapshot.InterruptMsg != "" {
			logrus.WithContext(ctx).WithField("task_id", tw.taskCtx.TaskID).Infof("task interrupted: %s", snapshot.InterruptMsg)
		}
		if serr := tw.SetStatus(ctx, database.TaskStatusWaiting); serr != nil {
			return fmt.Errorf("failed to set task %d waiting after interrupt: %w", tw.taskCtx.TaskID, serr)
		}
		return nil
	}

	if snapshot.IsCompleted {
		// LangGraph graph has finished. The ma_completed node in Python may have
		// already called Go's CompleteTask endpoint. Check DB status to avoid a
		// redundant second completion pass.
		currentStatus, _ := tw.GetStatus(ctx)
		if currentStatus != database.TaskStatusFinished && currentStatus != database.TaskStatusFailed {
			if cerr := tw.CompleteTask(ctx); cerr != nil {
				return fmt.Errorf("failed to complete task %d after langgraph completion: %w", tw.taskCtx.TaskID, cerr)
			}
		}
	}

	return nil
}

// ========================================
// Multi-agent migration: new method implementations
// ========================================

func (tw *taskWorker) DesignerStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "designer", msgChainID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute designer step: %w", err)
	}
	return decision, nil
}

func (tw *taskWorker) PlannerStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "planner", msgChainID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute planner step: %w", err)
	}
	if decision.Action == orchestrator.SupervisorActionPlanReady {
		plan, err := tw.todoPlanFromPlannerDecision(ctx, decision.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to persist planner todo plan: %w", err)
		}
		if _, err := tw.replaceTodoPlan(ctx, plan); err != nil {
			return nil, fmt.Errorf("failed to persist planner todo plan: %w", err)
		}
	}
	return decision, nil
}

func (tw *taskWorker) SupervisorStep(ctx context.Context, msgChainID int64) (*orchestrator.SupervisorDecision, error) {
	decision, err := tw.taskCtx.Provider.DecideSupervisorStep(ctx, tw.taskCtx.TaskID, "supervisor", msgChainID)
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
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	resolvedTodoID, resolvedTodo, err := tw.resolveAgentTodo(ctx, ma, agentRole, todoID, payload)
	if err != nil {
		logrus.WithContext(ctx).WithError(err).WithFields(logrus.Fields{
			"task_id":    tw.taskCtx.TaskID,
			"agent_role": agentRole,
			"todo_id":    todoID,
		}).Warn("failed to resolve multi-agent todo for delegated execution")
	}
	todoID = resolvedTodoID
	if todoID == "" || resolvedTodo == nil {
		return nil, fmt.Errorf("cannot execute agent %s without a valid todo_id", agentRole)
	}

	if err := ma.UpdateTodoStatus(ctx, tw.taskCtx.TaskID, todoID, string(orchestrator.TodoStatusInProgress), ""); err != nil {
		return nil, fmt.Errorf("failed to mark todo %s in progress: %w", todoID, err)
	}
	if err := ma.UpdateLegacySubtaskForTodo(ctx, tw.taskCtx.TaskID, todoID, string(orchestrator.TodoStatusInProgress), ""); err != nil {
		return nil, fmt.Errorf("failed to sync legacy subtask %s in progress: %w", todoID, err)
	}
	if err := ma.UpdateTaskSharedState(ctx, tw.taskCtx.TaskID, sqlPtrString(agentRole), sqlPtrString(todoID), nil, nil); err != nil {
		return nil, fmt.Errorf("failed to set active todo %s: %w", todoID, err)
	}
	if err := tw.publishTaskUpdate(ctx); err != nil {
		return nil, err
	}

	result, err := tw.taskCtx.Provider.ExecuteDelegatedAgent(ctx, tw.taskCtx.TaskID, 0, agentRole, payload)
	if err != nil {
		writeErr := tw.persistAgentExecutionResult(ctx, ma, agentRole, todoID, resolvedTodo, string(orchestrator.TodoStatusFailed), err.Error())
		if writeErr != nil {
			return nil, fmt.Errorf("failed to execute agent %s: %w; also failed to persist failure for todo %s: %v", agentRole, err, todoID, writeErr)
		}
		return nil, fmt.Errorf("failed to execute agent %s: %w", agentRole, err)
	}
	if result == nil {
		err := fmt.Errorf("agent %s returned nil execution result", agentRole)
		writeErr := tw.persistAgentExecutionResult(ctx, ma, agentRole, todoID, resolvedTodo, string(orchestrator.TodoStatusFailed), err.Error())
		if writeErr != nil {
			return nil, fmt.Errorf("%w; also failed to persist failure for todo %s: %v", err, todoID, writeErr)
		}
		return nil, err
	}

	status := string(orchestrator.TodoStatusCompleted)
	output := result.Result
	if !result.Success {
		status = string(orchestrator.TodoStatusFailed)
		output = result.Error
	}
	if output == "" {
		output = result.Result
	}
	if err := tw.persistAgentExecutionResult(ctx, ma, agentRole, todoID, resolvedTodo, status, output); err != nil {
		return nil, err
	}
	return result, nil
}

func (tw *taskWorker) persistAgentExecutionResult(
	ctx context.Context,
	ma *database.MultiAgentQueries,
	agentRole, todoID string,
	todo *database.Todo,
	status, output string,
) error {
	var evidence *database.Evidence
	var finding *database.Finding
	if strings.TrimSpace(output) != "" {
		evidenceType := "agent_result"
		if status == string(orchestrator.TodoStatusFailed) {
			evidenceType = "agent_error"
		}
		evidence = &database.Evidence{
			TaskID:       tw.taskCtx.TaskID,
			TodoID:       sqlPtrString(todoID),
			EvidenceType: sqlPtrString(evidenceType),
			Description:  sqlPtrString(fmt.Sprintf("%s result:\n%s", agentRole, output)),
		}
		if status == string(orchestrator.TodoStatusCompleted) && shouldStoreFindingForRole(agentRole) {
			finding = &database.Finding{
				TaskID:      tw.taskCtx.TaskID,
				TodoID:      sqlPtrString(todoID),
				FindingType: sqlPtrString("agent_result"),
				Severity:    sqlPtrString(todoSeverity(todo)),
				Title:       todoFindingTitle(todo),
				Description: sqlPtrString(output),
				Evidence:    json.RawMessage(`[]`),
				RawOutput:   sqlPtrString(output),
			}
		}
	}
	if err := ma.PersistTodoExecution(ctx, tw.taskCtx.TaskID, todoID, status, output, evidence, finding); err != nil {
		return fmt.Errorf("failed to persist execution result for todo %s: %w", todoID, err)
	}
	if err := tw.publishTaskUpdate(ctx); err != nil {
		return err
	}
	return nil
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
	authIDInt, err := strconv.ParseInt(authID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid auth ID %q: %w", authID, err)
	}
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	return ma.ResolveAuthRequest(ctx, authIDInt, status, response)
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
		Evidence:    json.RawMessage(`[]`),
		RawOutput:   sqlPtrString(rawOutput),
	})
}

func (tw *taskWorker) RejectTask(ctx context.Context, result string) error {
	return tw.finalizeTask(ctx, database.TaskStatusFailed, "REJECTED: "+result)
}

func (tw *taskWorker) CompleteTask(ctx context.Context) error {
	if tw.taskCtx.Orchestrator != nil {
		return tw.completeMultiAgentTask(ctx)
	}
	return tw.ReportTaskResult(ctx)
}

func (tw *taskWorker) completeMultiAgentTask(ctx context.Context) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	todos, err := ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to load todos for task %d completion: %w", tw.taskCtx.TaskID, err)
	}
	findings, err := ma.GetFindingsByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to load findings for task %d completion: %w", tw.taskCtx.TaskID, err)
	}
	evidence, err := ma.GetEvidenceByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return fmt.Errorf("failed to load evidence for task %d completion: %w", tw.taskCtx.TaskID, err)
	}

	repairedTodos := repairCompletedTodosFromStructuredOutput(todos, findings, evidence)
	if len(repairedTodos) > 0 {
		for _, repaired := range repairedTodos {
			if err := ma.UpdateTodoStatus(ctx, tw.taskCtx.TaskID, repaired.TodoID, repaired.Status, nullStringToString(repaired.Result)); err != nil {
				return fmt.Errorf("failed to repair todo %s before completion: %w", repaired.TodoID, err)
			}
			if err := ma.UpdateLegacySubtaskForTodo(ctx, tw.taskCtx.TaskID, repaired.TodoID, repaired.Status, nullStringToString(repaired.Result)); err != nil {
				return fmt.Errorf("failed to repair legacy subtask %s before completion: %w", repaired.TodoID, err)
			}
		}
		todos = mergeRepairedTodos(todos, repairedTodos)
	}

	result := multiAgentCompletionResult(tw.taskCtx.TaskTitle, tw.taskCtx.TaskInput, todos, findings, evidence)
	status, err := multiAgentCompletionStatus(todos)
	if err != nil {
		return err
	}
	if err := ma.ClearTaskActiveTodo(ctx, tw.taskCtx.TaskID); err != nil {
		return fmt.Errorf("failed to clear active todo for completed task %d: %w", tw.taskCtx.TaskID, err)
	}
	return tw.finalizeTask(ctx, status, result)
}

func (tw *taskWorker) UpdateSharedState(ctx context.Context, activeNode, activeTodoID string, statusCode *int, updates map[string]interface{}) error {
	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	var sharedState json.RawMessage
	if updates != nil {
		encodedState, err := json.Marshal(updates)
		if err != nil {
			return fmt.Errorf("failed to encode shared state update: %w", err)
		}
		sharedState = encodedState
	}
	return ma.UpdateTaskSharedState(
		ctx,
		tw.taskCtx.TaskID,
		sqlPtrString(activeNode),
		sqlPtrString(activeTodoID),
		statusCode,
		sharedState,
	)
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

	if err := tw.publishTaskUpdate(ctx); err != nil {
		return nil, err
	}

	return ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
}

func (tw *taskWorker) todoPlanFromPlannerDecision(ctx context.Context, rawResult string) ([]tools.TodoItem, error) {
	if rawResult == "" {
		return nil, fmt.Errorf("planner returned empty result")
	}

	var list tools.TodoListAction
	if err := json.Unmarshal([]byte(rawResult), &list); err == nil && len(list.Todos) > 0 {
		return list.Todos, nil
	}

	var patch tools.TodoPatchAction
	if err := json.Unmarshal([]byte(rawResult), &patch); err != nil {
		return nil, fmt.Errorf("planner result is neither todo_list nor todo_patch: %w", err)
	}
	if len(patch.Operations) == 0 {
		return nil, fmt.Errorf("planner todo_patch contains no operations")
	}

	ma := database.NewMultiAgentQueries(tw.taskCtx.RawDB)
	existing, err := ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing todos for planner patch: %w", err)
	}
	plan, err := providers.ApplyTodoOperations(dbTodosToToolItems(existing), patch, logrus.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (tw *taskWorker) publishTaskUpdate(ctx context.Context) error {
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

func dbTodosToToolItems(todos []database.Todo) []tools.TodoItem {
	items := make([]tools.TodoItem, 0, len(todos))
	for _, todo := range todos {
		items = append(items, tools.TodoItem{
			TodoID:               todo.TodoID,
			Title:                todo.Title,
			OwnerAgent:           todo.OwnerAgent,
			DependsOn:            decodeStringSlice(todo.DependsOn),
			NeedEnv:              todo.NeedEnv,
			NeedCode:             todo.NeedCode,
			RiskLevel:            todo.RiskLevel,
			AuthRequired:         todo.AuthRequired,
			Inputs:               nullStringToString(todo.Inputs),
			SuccessCriteria:      nullStringToString(todo.SuccessCriteria),
			EvidenceRequirements: decodeStringSlice(todo.EvidenceRequirements),
			Status:               todo.Status,
		})
	}
	return items
}

func decodeStringSlice(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil
	}
	return values
}

func nullStringToString(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func (tw *taskWorker) resolveAgentTodo(
	ctx context.Context,
	ma *database.MultiAgentQueries,
	agentRole, todoID string,
	payload json.RawMessage,
) (string, *database.Todo, error) {
	todos, err := ma.GetTodosByTaskID(ctx, tw.taskCtx.TaskID)
	if err != nil {
		return todoID, nil, err
	}

	if todoID == "" {
		todoID = extractTodoIDFromPayload(payload)
	}
	if todoID != "" {
		return todoID, findTodoByID(todos, todoID), nil
	}

	role := normalizeAgentRole(agentRole)
	for _, todo := range todos {
		if normalizeAgentRole(todo.OwnerAgent) == role && isOpenTodoStatus(todo.Status) {
			return todo.TodoID, &todo, nil
		}
	}
	for _, todo := range todos {
		if normalizeAgentRole(todo.OwnerAgent) == role {
			return todo.TodoID, &todo, nil
		}
	}
	for _, todo := range todos {
		if isOpenTodoStatus(todo.Status) {
			return todo.TodoID, &todo, nil
		}
	}

	return "", nil, nil
}

func extractTodoIDFromPayload(payload json.RawMessage) string {
	if len(payload) == 0 {
		return ""
	}
	var value interface{}
	if err := json.Unmarshal(payload, &value); err != nil {
		return scanTodoID(string(payload))
	}
	return extractTodoIDValue(value)
}

func extractTodoIDValue(value interface{}) string {
	if id := extractExplicitTodoIDValue(value); id != "" {
		return id
	}
	return scanTodoIDValue(value)
}

func extractExplicitTodoIDValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}:
		for _, key := range []string{"todo_id", "active_todo_id"} {
			if id := scanTodoID(fmt.Sprint(v[key])); id != "" {
				return id
			}
		}
		for _, item := range v {
			if id := extractExplicitTodoIDValue(item); id != "" {
				return id
			}
		}
	case []interface{}:
		for _, item := range v {
			if id := extractExplicitTodoIDValue(item); id != "" {
				return id
			}
		}
	}
	return ""
}

func scanTodoIDValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}:
		for _, item := range v {
			if id := scanTodoIDValue(item); id != "" {
				return id
			}
		}
	case []interface{}:
		for _, item := range v {
			if id := scanTodoIDValue(item); id != "" {
				return id
			}
		}
	case string:
		return scanTodoID(v)
	}
	return ""
}

func scanTodoID(value string) string {
	idx := strings.Index(value, "todo_")
	if idx < 0 {
		return ""
	}
	end := idx + len("todo_")
	for end < len(value) {
		ch := value[end]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			end++
			continue
		}
		break
	}
	return value[idx:end]
}

func findTodoByID(todos []database.Todo, todoID string) *database.Todo {
	for _, todo := range todos {
		if todo.TodoID == todoID {
			return &todo
		}
	}
	return nil
}

func normalizeAgentRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "coder", "code", "developer":
		return "generator"
	case "installer", "builder":
		return "builder"
	case "pentest", "tester", "security_tester":
		return "pentester"
	default:
		return strings.ToLower(strings.TrimSpace(role))
	}
}

func isOpenTodoStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "created", "running", "waiting", string(orchestrator.TodoStatusPending), string(orchestrator.TodoStatusInProgress), string(orchestrator.TodoStatusBlocked):
		return true
	default:
		return false
	}
}

func isPentesterRole(role string) bool {
	return normalizeAgentRole(role) == "pentester"
}

func shouldStoreFindingForRole(role string) bool {
	switch normalizeAgentRole(role) {
	case "pentester", "tester", "reviewer":
		return true
	default:
		return false
	}
}

func todoSeverity(todo *database.Todo) string {
	if todo == nil || strings.TrimSpace(todo.RiskLevel) == "" {
		return "info"
	}
	return todo.RiskLevel
}

func todoFindingTitle(todo *database.Todo) string {
	if todo == nil || strings.TrimSpace(todo.Title) == "" {
		return "Security test result"
	}
	return "Security test result: " + todo.Title
}

func multiAgentCompletionResult(
	title, input string,
	todos []database.Todo,
	findings []database.Finding,
	evidence []database.Evidence,
) string {
	var b strings.Builder
	b.WriteString("# ")
	if strings.TrimSpace(title) != "" {
		b.WriteString(title)
	} else {
		b.WriteString("Multi-agent task report")
	}
	b.WriteString("\n\n")

	if strings.TrimSpace(input) != "" {
		b.WriteString("## Original Prompt\n")
		b.WriteString(input)
		b.WriteString("\n\n")
	}

	if reporterResult := latestReporterResult(todos); reporterResult != "" {
		b.WriteString("## Reporter Summary\n")
		b.WriteString(reporterResult)
		b.WriteString("\n\n")
	}

	b.WriteString("## Todos\n")
	if len(todos) == 0 {
		b.WriteString("- No todos were recorded.\n")
	} else {
		for _, todo := range todos {
			b.WriteString("- ")
			b.WriteString(todo.TodoID)
			b.WriteString(" [")
			b.WriteString(todo.Status)
			b.WriteString("] ")
			b.WriteString(todo.Title)
			if todo.Result.Valid && strings.TrimSpace(todo.Result.String) != "" {
				b.WriteString("\n  Result: ")
				b.WriteString(collapseWhitespace(todo.Result.String))
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")

	b.WriteString("## Findings\n")
	if len(findings) == 0 {
		b.WriteString("- No findings were recorded.\n")
	} else {
		for _, finding := range findings {
			b.WriteString("- ")
			if finding.Severity.Valid && finding.Severity.String != "" {
				b.WriteString("[")
				b.WriteString(finding.Severity.String)
				b.WriteString("] ")
			}
			b.WriteString(finding.Title)
			if finding.TodoID.Valid && finding.TodoID.String != "" {
				b.WriteString(" (")
				b.WriteString(finding.TodoID.String)
				b.WriteString(")")
			}
			if finding.Description.Valid && strings.TrimSpace(finding.Description.String) != "" {
				b.WriteString("\n  Description: ")
				b.WriteString(collapseWhitespace(finding.Description.String))
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")

	b.WriteString("## Evidence\n")
	if len(evidence) == 0 {
		b.WriteString("- No evidence was recorded.\n")
	} else {
		for _, item := range evidence {
			b.WriteString("- ")
			if item.TodoID.Valid && item.TodoID.String != "" {
				b.WriteString(item.TodoID.String)
				b.WriteString(": ")
			}
			if item.EvidenceType.Valid && item.EvidenceType.String != "" {
				b.WriteString(item.EvidenceType.String)
			} else {
				b.WriteString("evidence")
			}
			if item.Description.Valid && strings.TrimSpace(item.Description.String) != "" {
				b.WriteString(" - ")
				b.WriteString(collapseWhitespace(item.Description.String))
			}
			b.WriteString("\n")
		}
	}

	return b.String()
}

func multiAgentCompletionStatus(todos []database.Todo) (database.TaskStatus, error) {
	if len(todos) == 0 {
		return database.TaskStatusFinished, nil
	}
	hasFailed := false
	openTodos := make([]string, 0)
	for _, todo := range todos {
		switch strings.ToLower(strings.TrimSpace(todo.Status)) {
		case string(orchestrator.TodoStatusCompleted), string(orchestrator.TodoStatusSkipped), "finished", "done", "success":
		case string(orchestrator.TodoStatusFailed), "error", "rejected":
			hasFailed = true
		default:
			openTodos = append(openTodos, todo.TodoID)
		}
	}
	if len(openTodos) > 0 {
		return database.TaskStatusRunning, fmt.Errorf(
			"cannot complete task %d: open todos remain: %s",
			todos[0].TaskID,
			strings.Join(openTodos, ", "),
		)
	}
	if hasFailed {
		return database.TaskStatusFailed, nil
	}
	return database.TaskStatusFinished, nil
}

func repairCompletedTodosFromStructuredOutput(
	todos []database.Todo,
	findings []database.Finding,
	evidence []database.Evidence,
) []database.Todo {
	repaired := make([]database.Todo, 0)
	for _, todo := range todos {
		if !isOpenTodoStatus(todo.Status) || !todoHasStructuredOutput(todo, findings, evidence) {
			continue
		}
		todo.Status = string(orchestrator.TodoStatusCompleted)
		if !todo.Result.Valid || strings.TrimSpace(todo.Result.String) == "" {
			todo.Result = sql.NullString{
				String: todoStructuredOutputSummary(todo.TodoID, findings, evidence),
				Valid:  true,
			}
		}
		repaired = append(repaired, todo)
	}
	return repaired
}

func mergeRepairedTodos(todos, repaired []database.Todo) []database.Todo {
	if len(repaired) == 0 {
		return todos
	}
	byID := make(map[string]database.Todo, len(repaired))
	for _, todo := range repaired {
		byID[todo.TodoID] = todo
	}
	merged := make([]database.Todo, 0, len(todos))
	for _, todo := range todos {
		if repairedTodo, ok := byID[todo.TodoID]; ok {
			merged = append(merged, repairedTodo)
		} else {
			merged = append(merged, todo)
		}
	}
	return merged
}

func todoHasStructuredOutput(todo database.Todo, findings []database.Finding, evidence []database.Evidence) bool {
	if todo.Result.Valid && strings.TrimSpace(todo.Result.String) != "" {
		return true
	}
	for _, finding := range findings {
		if finding.TodoID.Valid && finding.TodoID.String == todo.TodoID {
			return true
		}
	}
	for _, item := range evidence {
		if item.TodoID.Valid && item.TodoID.String == todo.TodoID {
			return true
		}
	}
	return false
}

func todoStructuredOutputSummary(todoID string, findings []database.Finding, evidence []database.Evidence) string {
	for _, finding := range findings {
		if finding.TodoID.Valid && finding.TodoID.String == todoID {
			if finding.Description.Valid && strings.TrimSpace(finding.Description.String) != "" {
				return finding.Description.String
			}
			if strings.TrimSpace(finding.Title) != "" {
				return finding.Title
			}
		}
	}
	for _, item := range evidence {
		if item.TodoID.Valid && item.TodoID.String == todoID && item.Description.Valid && strings.TrimSpace(item.Description.String) != "" {
			return item.Description.String
		}
	}
	return "Structured evidence was recorded for this todo."
}

func latestReporterResult(todos []database.Todo) string {
	for i := len(todos) - 1; i >= 0; i-- {
		todo := todos[i]
		if normalizeAgentRole(todo.OwnerAgent) == "reporter" && todo.Result.Valid {
			if result := strings.TrimSpace(todo.Result.String); result != "" {
				return result
			}
		}
	}
	return ""
}

func collapseWhitespace(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
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
