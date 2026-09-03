package providers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"pentagentx/pkg/cast"
	"pentagentx/pkg/csum"
	"pentagentx/pkg/database"
	obs "pentagentx/pkg/observability"
	"pentagentx/pkg/observability/langfuse"
	"pentagentx/pkg/templates"
	"pentagentx/pkg/tools"

	"github.com/sirupsen/logrus"
)

type reporterTodoView struct {
	ID                   string
	Title                string
	OwnerAgent           string
	Status               string
	RiskLevel            string
	Inputs               string
	SuccessCriteria      string
	EvidenceRequirements []string
	Result               string
}

type reporterFindingView struct {
	ID          int64
	TodoID      string
	FindingType string
	Severity    string
	Title       string
	Description string
	RawOutput   string
}

type reporterEvidenceView struct {
	ID           int64
	TodoID       string
	ArtifactID   string
	EvidenceType string
	RelativePath string
	Description  string
	Hash         string
}

func (fp *flowProvider) GetReporterHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error) {
	ptrTask, ptrSubtask, err := fp.getTaskAndSubtask(ctx, taskID, subtaskID)
	if err != nil {
		return nil, err
	}

	executionContext, err := fp.getExecutionContext(ctx, taskID, subtaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution context: %w", err)
	}

	reporterHandler := func(ctx context.Context, input string) (string, error) {
		userContext, err := fp.buildReporterUserContext(ctx, taskID, ptrTask, executionContext)
		if err != nil {
			return "", fmt.Errorf("failed to build reporter context: %w", err)
		}

		reporterContext := map[string]map[string]any{
			"user": userContext,
			"system": {
				"ReportResultToolName":    tools.ReportResultToolName,
				"SummarizationToolName":   cast.SummarizationToolName,
				"SummarizedContentPrefix": csum.SummarizedContentPrefix,
				"Lang":                    fp.language,
				"N":                       "8000",
				"ToolPlaceholder":         ToolPlaceholder,
			},
		}

		reporterCtx, observation := obs.Observer.NewObservation(ctx)
		reporterEvaluator := observation.Evaluator(
			langfuse.WithEvaluatorName("render reporter agent prompts"),
			langfuse.WithEvaluatorInput(reporterContext),
			langfuse.WithEvaluatorMetadata(langfuse.Metadata{
				"user_context":   reporterContext["user"],
				"system_context": reporterContext["system"],
				"task":           ptrTask,
				"subtask":        ptrSubtask,
				"lang":           fp.language,
			}),
		)

		userReporterTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeTaskReporter, reporterContext["user"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(reporterCtx, reporterEvaluator, "failed to get user reporter template", err)
		}

		systemReporterTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeReporter, reporterContext["system"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(reporterCtx, reporterEvaluator, "failed to get system reporter template", err)
		}

		reporterEvaluator.End(
			langfuse.WithEvaluatorOutput(map[string]any{
				"user_template":   userReporterTmpl,
				"system_template": systemReporterTmpl,
			}),
			langfuse.WithEvaluatorStatus("success"),
			langfuse.WithEvaluatorLevel(langfuse.ObservationLevelDebug),
		)

		reporterResult, err := fp.performTaskResultReporter(ctx, taskID, subtaskID, systemReporterTmpl, userReporterTmpl, input)
		if err != nil {
			return "", wrapError(ctx, "failed to get reporter result", err)
		}

		resultJSON, err := json.Marshal(reporterResult)
		if err != nil {
			return "", wrapError(ctx, "failed to marshal reporter result", err)
		}

		return string(resultJSON), nil
	}

	return func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.getReporterHandler")
		defer span.End()

		logrus.WithContext(ctx).WithField("tool", name).Info("reporter agent execution started")

		input := string(args)
		if input == "" {
			input = "Generate final penetration test report"
		}

		reporterResult, err := reporterHandler(ctx, input)
		if err != nil {
			return "", err
		}

		return reporterResult, nil
	}, nil
}

func (fp *flowProvider) buildReporterUserContext(
	ctx context.Context,
	taskID *int64,
	ptrTask *database.Task,
	executionContext string,
) (map[string]any, error) {
	task := database.Task{}
	if ptrTask != nil {
		task = *ptrTask
	}

	previousTasks := []database.Task{}
	completedSubtasks := []database.Subtask{}
	plannedSubtasks := []database.Subtask{}
	todos := []reporterTodoView{}
	findings := []reporterFindingView{}
	evidence := []reporterEvidenceView{}

	if taskID != nil {
		tasksInfo, err := fp.getTasksInfo(ctx, *taskID)
		if err != nil {
			return nil, err
		}
		if tasksInfo.Task.ID != 0 {
			task = tasksInfo.Task
		}
		previousTasks = tasksInfo.Tasks

		subtasksInfo := fp.getSubtasksInfo(*taskID, tasksInfo.Subtasks)
		completedSubtasks = subtasksInfo.Completed
		plannedSubtasks = subtasksInfo.Planned

		ma, err := fp.multiAgentQueries()
		if err != nil {
			return nil, err
		}
		dbTodos, err := ma.GetTodosByTaskID(ctx, *taskID)
		if err != nil {
			return nil, fmt.Errorf("failed to load todos for reporter context: %w", err)
		}
		dbFindings, err := ma.GetFindingsByTaskID(ctx, *taskID)
		if err != nil {
			return nil, fmt.Errorf("failed to load findings for reporter context: %w", err)
		}
		dbEvidence, err := ma.GetEvidenceByTaskID(ctx, *taskID)
		if err != nil {
			return nil, fmt.Errorf("failed to load evidence for reporter context: %w", err)
		}
		todos = reporterTodos(dbTodos)
		findings = reporterFindings(dbFindings)
		evidence = reporterEvidence(dbEvidence)
	}

	return map[string]any{
		"Task":              task,
		"Tasks":             previousTasks,
		"CompletedSubtasks": completedSubtasks,
		"PlannedSubtasks":   plannedSubtasks,
		"Todos":             todos,
		"Findings":          findings,
		"Evidence":          evidence,
		"ExecutionLogs":     executionContext,
		"ExecutionState":    "",
	}, nil
}

func reporterTodos(todos []database.Todo) []reporterTodoView {
	views := make([]reporterTodoView, 0, len(todos))
	for _, todo := range todos {
		views = append(views, reporterTodoView{
			ID:                   todo.TodoID,
			Title:                todo.Title,
			OwnerAgent:           todo.OwnerAgent,
			Status:               todo.Status,
			RiskLevel:            todo.RiskLevel,
			Inputs:               reporterNullString(todo.Inputs),
			SuccessCriteria:      reporterNullString(todo.SuccessCriteria),
			EvidenceRequirements: decodeStringList(todo.EvidenceRequirements),
			Result:               reporterNullString(todo.Result),
		})
	}
	return views
}

func reporterFindings(findings []database.Finding) []reporterFindingView {
	views := make([]reporterFindingView, 0, len(findings))
	for _, finding := range findings {
		views = append(views, reporterFindingView{
			ID:          finding.ID,
			TodoID:      reporterNullString(finding.TodoID),
			FindingType: reporterNullString(finding.FindingType),
			Severity:    reporterNullString(finding.Severity),
			Title:       finding.Title,
			Description: reporterNullString(finding.Description),
			RawOutput:   reporterNullString(finding.RawOutput),
		})
	}
	return views
}

func reporterEvidence(evidence []database.Evidence) []reporterEvidenceView {
	views := make([]reporterEvidenceView, 0, len(evidence))
	for _, item := range evidence {
		views = append(views, reporterEvidenceView{
			ID:           item.ID,
			TodoID:       reporterNullString(item.TodoID),
			ArtifactID:   reporterNullString(item.ArtifactID),
			EvidenceType: reporterNullString(item.EvidenceType),
			RelativePath: reporterNullString(item.RelativePath),
			Description:  reporterNullString(item.Description),
			Hash:         reporterNullString(item.Hash),
		})
	}
	return views
}

func reporterNullString(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}
