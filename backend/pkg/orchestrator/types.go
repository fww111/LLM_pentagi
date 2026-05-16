package orchestrator

import "encoding/json"

type PrimaryAgentAction string

const (
	PrimaryAgentActionCallAgent     PrimaryAgentAction = "call_agent"
	PrimaryAgentActionInputRequired PrimaryAgentAction = "input_required"
	PrimaryAgentActionCompleted     PrimaryAgentAction = "completed"
	PrimaryAgentActionFailed        PrimaryAgentAction = "failed"
)

type PrimaryAgentDecision struct {
	Action     PrimaryAgentAction `json:"action"`
	AgentType  string             `json:"agent_type,omitempty"`
	Payload    json.RawMessage    `json:"payload,omitempty"`
	ToolCallID string             `json:"tool_call_id,omitempty"`
	Message    string             `json:"message,omitempty"`
	Result     string             `json:"result,omitempty"`
	Error      string             `json:"error,omitempty"`
	MsgChainID int64              `json:"msg_chain_id,omitempty"`
}

type AgentExecutionResult struct {
	AgentType string `json:"agent_type"`
	Success   bool   `json:"success"`
	Result    string `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
}
