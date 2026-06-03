 package providers

  import (
        "context"
        "encoding/json"
        "fmt"

        "pentagi/pkg/cast"
        "pentagi/pkg/csum"
        "pentagi/pkg/database"
        "pentagi/pkg/docker"
        obs "pentagi/pkg/observability"
        "pentagi/pkg/observability/langfuse"
        "pentagi/pkg/providers/pconfig"
        "pentagi/pkg/schema"
        "pentagi/pkg/templates"
        "pentagi/pkg/tools"

        "github.com/sirupsen/logrus"
  )

  func (fp *flowProvider) GetReviewerHandler(ctx context.Context, taskID, subtaskID *int64)
  (tools.ExecutorHandler, error) {
        ptrTask, ptrSubtask, err := fp.getTaskAndSubtask(ctx, taskID, subtaskID)
        if err != nil {
                return nil, err
        }

        executionContext, err := fp.getExecutionContext(ctx, taskID, subtaskID)
        if err != nil {
                return nil, fmt.Errorf("failed to get execution context: %w", err)
        }

        reviewerHandler := func(ctx context.Context, action tools.ReviewerAction) (string, error) {
                reviewerContext := map[string]map[string]any{
                        "user": {
                                "ScopeContract": action.ScopeContract,
                                "Plan":         action.Plan,
                                "Findings":     action.Findings,
                                "Evidence":     action.Evidence,
                        },
                        "system": {
                                "ReviewResultToolName":     tools.ReviewResultToolName,
                                "SummarizationToolName":    cast.SummarizationToolName,
                                "SummarizedContentPrefix":  strings.ReplaceAll(csum.SummarizedContentPrefix, "\n
                                "GraphitiEnabled":          fp.graphitiClient != nil && fp.graphitiClient.IsEnab
                                "GraphitiSearchToolName":   tools.GraphitiSearchToolName,
                                "SearchToolName":           tools.SearchToolName,
                                "AdviceToolName":           tools.AdviceToolName,
                                "MemoristToolName":         tools.MemoristToolName,
                                "TerminalToolName":         tools.TerminalToolName,
                                "FileToolName":             tools.FileToolName,
                                "BrowserToolName":          tools.BrowserToolName,
                                "DockerImage":              fp.image,
                                "Cwd":                      docker.WorkFolderPathInContainer,
                                "ContainerPorts":           fp.getContainerPortsDescription(),
                                "ExecutionContext":         executionContext,
                                "Lang":                     fp.language,
                                "CurrentTime":              getCurrentTime(),
                                "ToolPlaceholder":          ToolPlaceholder,
                        },
                }

                reviewerCtx, observation := obs.Observer.NewObservation(ctx)
                reviewerEvaluator := observation.Evaluator(
                        langfuse.WithEvaluatorName("render reviewer agent prompts"),
                        langfuse.WithEvaluatorInput(reviewerContext),
                        langfuse.WithEvaluatorMetadata(langfuse.Metadata{
                                "user_context":   reviewerContext["user"],
                                "system_context": reviewerContext["system"],
                                "task":           ptrTask,
                                "subtask":        ptrSubtask,
                                "lang":           fp.language,
                        }),
                )

                userReviewerTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeQuestionReviewer,
  reviewerContext["user"])
                if err != nil {
                        return "", wrapErrorEndEvaluatorSpan(reviewerCtx, reviewerEvaluator, "failed to get user
  err)
                }

                systemReviewerTmpl, err := fp.prompter.RenderTemplate(templates.PromptTypeReviewer, reviewerCont
                if err != nil {
                        return "", wrapErrorEndEvaluatorSpan(reviewerCtx, reviewerEvaluator, "failed to get syst
   err)
                }

                reviewerEvaluator.End(
                        langfuse.WithEvaluatorOutput(map[string]any{
                                "user_template":   userReviewerTmpl,
                                "system_template": systemReviewerTmpl,
                        }),
                        langfuse.WithEvaluatorStatus("success"),
                        langfuse.WithEvaluatorLevel(langfuse.ObservationLevelDebug),
                )

                reviewerResult, err := fp.performReviewer(ctx, taskID, subtaskID, systemReviewerTmpl, userReview
  action.ScopeContract, action.Plan, action.Findings, action.Evidence)
                if err != nil {
                        return "", wrapError(ctx, "failed to get reviewer result", err)
                }

                return reviewerResult, nil
        }

        return func(ctx context.Context, name string, args json.RawMessage) (string, error) {
                ctx, span := obs.Observer.NewSpan(ctx, obs.SpanKindInternal, "providers.flowProvider.getReviewer
                defer span.End()

                var action tools.ReviewerAction
                if err := json.Unmarshal(args, &action); err != nil {
                        logrus.WithContext(ctx).WithError(err).Error("failed to unmarshal reviewer payload")
                        return "", fmt.Errorf("failed to unmarshal reviewer payload: %w", err)
                }

                reviewerResult, err := reviewerHandler(ctx, action)
                if err != nil {
                        return "", err
                }

                return reviewerResult, nil
        }, nil
  }