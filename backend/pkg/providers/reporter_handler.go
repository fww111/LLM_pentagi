package providers

import (
	"context"
	"encoding/json"
	"fmt"

	"pentagi/pkg/cast"
	"pentagi/pkg/csum"
	obs "pentagi/pkg/observability"
	"pentagi/pkg/observability/langfuse"
	"pentagi/pkg/templates"
	"pentagi/pkg/tools"

	"github.com/sirupsen/logrus"
)

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
		reporterContext := map[string]map[string]any{
			"user": {
				"Task":               ptrTask,
				"Tasks":              ptrTask,
				"CompletedSubtasks":  "",
				"PlannedSubtasks":    "",
				"ExecutionLogs":      executionContext,
				"ExecutionState":     "",
			},
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
