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

func (fp *flowProvider) GetIntegratorHandler(ctx context.Context, taskID, subtaskID *int64) (tools.ExecutorHandler, error) {
	ptrTask, ptrSubtask, err := fp.getTaskAndSubtask(ctx, taskID, subtaskID)
	if err != nil {
		return nil, err
	}

	executionContext, err := fp.getExecutionContext(ctx, taskID, subtaskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution context: %w", err)
	}

	integratorHandler := func(ctx context.Context, action tools.IntegratorAction) (string, error) {
		integratorContext := map[string]map[string]any{
			"user": {
				"Question": action.Question,
			},
			"system": {
				"IntegrationResultToolName": tools.IntegrationResultToolName,
				"SearchCodeToolName":        tools.SearchCodeToolName,
				"StoreCodeToolName":         tools.StoreCodeToolName,
				"GraphitiEnabled":           fp.graphitiClient != nil && fp.graphitiClient.IsEnabled(),
				"GraphitiSearchToolName":    tools.GraphitiSearchToolName,
				"SearchToolName":            tools.SearchToolName,
				"AdviceToolName":            tools.AdviceToolName,
				"MemoristToolName":          tools.MemoristToolName,
				"SummarizationToolName":     cast.SummarizationToolName,
				"SummarizedContentPrefix":   strings.ReplaceAll(csum.SummarizedContentPrefix, "\n", "\\n"),
				"DockerImage":               fp.image,
				"Cwd":                       docker.WorkFolderPathInContainer,
				"ContainerPorts":            fp.getContainerPortsDescription(),
				"ExecutionContext":          executionContext,
				"Lang":                      fp.language,
				"CurrentTime":               getCurrentTime(),
				"ToolPlaceholder":           ToolPlaceholder,
			},
		}

		integratorCtx, observation := obs.Observer.NewObservation(ctx)
		integratorEvaluator := observation.Evaluator(
			langfuse.WithEvaluatorName("render integrator agent prompts"),
			langfuse.WithEvaluatorInput(integratorContext),
			langfuse.WithEvaluatorMetadata(langfuse.Metadata{
				"user_context":   integratorContext["user"],
				"system_context": integratorContext["system"],
				"task":           ptrTask,
				"subtask":        ptrSubtask,
				"lang":           fp.language,
			}),
		)

		userIntegratorTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeQuestionIntegrator, integratorContext["user"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(integratorCtx, integratorEvaluator, "failed to get user integrator template", err)
		}

		systemIntegratorTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeIntegrator, integratorContext["system"])
		if err != nil {
			return "", wrapErrorEndEvaluatorSpan(integratorCtx, integratorEvaluator, "failed to get system integrator template", err)
		}

		integratorEvaluator.End(
			langfuse.WithEvaluatorOutput(map[string]any{
				"user_template":   userIntegratorTmpl,
				"system_template": systemIntegratorTmpl,
			}),
			langfuse.WithEvaluatorStatus("success"),
			langfuse.WithEvaluatorLevel(langfuse.ObservationLevelDebug),
		)

		result, err := fp.performIntegrator(
			ctx,
			taskID,
			subtaskID,
			systemIntegratorTmpl,
			userIntegratorTmpl,
			action.Question,
		)
		if err != nil {
			return "", wrapError(ctx, "failed to get integrator result", err)
		}

		return result, nil
	}

	return func(ctx context.Context, name string, args json.RawMessage) (string, error) {
		ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.getIntegratorHandler")
		defer span.End()

		var action tools.IntegratorAction
		if err := json.Unmarshal(args, &action); err != nil {
			logrus.WithContext(ctx).WithError(err).Error("failed to unmarshal integrator payload")
			return "", fmt.Errorf("failed to unmarshal integrator payload: %w", err)
		}

		return integratorHandler(ctx, action)
	}, nil
}
