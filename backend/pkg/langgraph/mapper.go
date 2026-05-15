package langgraph

type TaskRuntimeInput struct {
	FlowID    int64
	TaskID    int64
	ContextID string
	Content   string
}

func NewStartRunRequest(input TaskRuntimeInput, provider ProviderProfile, workspace WorkspaceRef) StartRunRequest {
	return StartRunRequest{
		TaskID:    input.TaskID,
		ContextID: input.ContextID,
		Content:   input.Content,
		Provider:  provider,
		Workspace: workspace,
		Data: map[string]any{
			"flow_id": input.FlowID,
		},
	}
}
