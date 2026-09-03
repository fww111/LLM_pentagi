package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"pentagentx/pkg/cast"
	"pentagentx/pkg/csum"
	"pentagentx/pkg/docker"
	obs "pentagentx/pkg/observability"
	"pentagentx/pkg/observability/langfuse"
	"pentagentx/pkg/templates"
	"pentagentx/pkg/tools"

	"github.com/sirupsen/logrus"
)

func (fp *flowProvider) GetTesterHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error) {
	ptrTask, ptrSubtask, err := fp.getTaskAndSubtask(ctx, taskID, subtaskID)
	if err != nil {
		return nil, err
	}

	executionContext, err := fp.getExecutionContext(ctx, taskID, subtaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution context: %w", err)
	}

	testerHandler := func(ctx context.Context, action tools.TesterAction) (string, error) {
		testerContext := map[string]map[string]any{
			"user": {
				"Question": action.Question,
			},
			"system": {
				"TestResultToolName":      tools.TestResultToolName,
				"SearchGuideToolName":     tools.SearchGuideToolName,
				"StoreGuideToolName":      tools.StoreGuideToolName,
				"GraphitiEnabled":         fp.graphitiClient != nil && fp.graphitiClient.IsEnabled(),
				"GraphitiSearchToolName":  tools.GraphitiSearchToolName,
				"SearchToolName":          tools.SearchToolName,
				"AdviceToolName":          tools.AdviceToolName,
				"MemoristToolName":        tools.MemoristToolName,
				"SummarizationToolName":   cast.SummarizationToolName,
				"SummarizedContentPrefix": strings.ReplaceAll(csum.SummarizedContentPrefix, "\n", "\\n"),
				"DockerImage":             fp.image,
				"Cwd":                     docker.WorkFolderPathInContainer,
				"ContainerPorts":          fp.getContainerPortsDescription(),
				"ExecutionContext":        executionContext,
				"Lang":                    fp.language,
				"CurrentTime":             getCurrentTime(),
				"ToolPlaceholder":         ToolPlaceholder,
			},
		}

		testerCtx, observation := obs.Observer.NewObservation(ctx)
		testerEvaluator := observation.Evaluator(
			langfuse.WithEvaluatorName("render tester agent prompts"),
			langfuse.WithEvaluatorInput(testerContext),
			langfuse.WithEvaluatorMetadata(langfuse.Metadata{
				"user_context":   testerContext["user"],
				"system_context": testerContext["system"],
				"task":           ptrTask,
				"subtask":        ptrSubtask,
				"lang":           fp.language,
			}),
		)

		userTesterTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeQuestionTester, testerContext["user"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(testerCtx, testerEvaluator, "failed to get user tester template", err)
		}

		systemTesterTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeTester, testerContext["system"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(testerCtx, testerEvaluator, "failed to get system tester template", err)
		}

		testerEvaluator.End(
			langfuse.WithEvaluatorOutput(map[string]any{
				"user_template":   userTesterTmpl,
				"system_template": systemTesterTmpl,
			}),
			langfuse.WithEvaluatorStatus("success"),
			langfuse.WithEvaluatorLevel(langfuse.ObservationLevelDebug),
		)

		result, err := fp.performTester(
			ctx,
			taskID,
			subtaskID,
			systemTesterTmpl,
			userTesterTmpl,
			action.Question,
		)
		if err != nil {
			return "", wrapError(ctx, "failed to get tester result", err)
		}

		return result, nil
	}

	return func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.getTesterHandler")
		defer span.End()

		var action tools.TesterAction
		if err := json.Unmarshal(args, &action); err != nil {
			logrus.WithContext(ctx).WithError(err).Error("failed to unmarshal tester payload")
			return "", fmt.Errorf("failed to unmarshal tester payload: %w", err)
		}

		return testerHandler(ctx, action)
	}, nil
}
