package langgraph

type WorkspaceRef struct {
	FlowID        int64  `json:"flow_id"`
	TaskID        int64  `json:"task_id"`
	ContextID     string `json:"context_id"`
	HostPath      string `json:"host_path,omitempty"`
	ContainerPath string `json:"container_path,omitempty"`
}

type ProviderProfile struct {
	ProviderType  string `json:"provider_type,omitempty"`
	Model         string `json:"model,omitempty"`
	SupportsTools bool   `json:"supports_tools"`
}

type StartRunRequest struct {
	TaskID    int64                  `json:"task_id"`
	ContextID string                 `json:"context_id"`
	Content   string                 `json:"content"`
	Data      map[string]any         `json:"data,omitempty"`
	Provider  ProviderProfile        `json:"provider"`
	Workspace WorkspaceRef           `json:"workspace"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type StartRunResponse struct {
	RunID     string `json:"run_id,omitempty"`
	ContextID string `json:"context_id"`
	Status    string `json:"status"`
}

type ResumeRunRequest struct {
	ContextID     string `json:"context_id"`
	ResumePayload any    `json:"resume_payload"`
}

type ResumeRunResponse struct {
	ContextID string `json:"context_id"`
	Status    string `json:"status"`
}

type CancelRunRequest struct {
	ContextID string `json:"context_id"`
	Reason    string `json:"reason,omitempty"`
}

type StateSnapshot struct {
	ContextID string         `json:"context_id"`
	State     map[string]any `json:"state"`
}
