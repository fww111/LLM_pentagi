package orchestrator

import "encoding/json"

// ========================================
// Legacy types (existing)
// ========================================

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

// ========================================
// Multi-agent migration: Agent roles
// ========================================

type AgentRole string

const (
	AgentRoleDesigner    AgentRole = "designer"
	AgentRolePlanner     AgentRole = "planner"
	AgentRoleSupervisor  AgentRole = "supervisor"
	AgentRoleBuilder     AgentRole = "builder"
	AgentRoleGenerator   AgentRole = "generator"
	AgentRoleIntegrator  AgentRole = "integrator"
	AgentRoleTester      AgentRole = "tester"
	AgentRolePentester   AgentRole = "pentester"
	AgentRoleReviewer    AgentRole = "reviewer"
	AgentRoleReporter    AgentRole = "reporter"
	AgentRoleResearcher  AgentRole = "researcher"
)

// AllAgentRoles returns the list of all valid agent roles
func AllAgentRoles() []AgentRole {
	return []AgentRole{
		AgentRoleDesigner,
		AgentRolePlanner,
		AgentRoleSupervisor,
		AgentRoleBuilder,
		AgentRoleGenerator,
		AgentRoleIntegrator,
		AgentRoleTester,
		AgentRolePentester,
		AgentRoleReviewer,
		AgentRoleReporter,
		AgentRoleResearcher,
	}
}

// ========================================
// Multi-agent migration: Task state enum
// ========================================

type TaskStateCode int

const (
	TaskStateUnspecified   TaskStateCode = 0
	TaskStateSubmitted     TaskStateCode = 1
	TaskStateWorking       TaskStateCode = 2
	TaskStateCompleted     TaskStateCode = 3
	TaskStateFailed        TaskStateCode = 4
	TaskStateCanceled      TaskStateCode = 5
	TaskStateInputRequired TaskStateCode = 6
	TaskStateRejected      TaskStateCode = 7
	TaskStateAuthRequired  TaskStateCode = 8
)

func (t TaskStateCode) String() string {
	switch t {
	case TaskStateSubmitted:
		return "SUBMITTED"
	case TaskStateWorking:
		return "WORKING"
	case TaskStateCompleted:
		return "COMPLETED"
	case TaskStateFailed:
		return "FAILED"
	case TaskStateCanceled:
		return "CANCELED"
	case TaskStateInputRequired:
		return "INPUT_REQUIRED"
	case TaskStateRejected:
		return "REJECTED"
	case TaskStateAuthRequired:
		return "AUTH_REQUIRED"
	default:
		return "UNSPECIFIED"
	}
}

// ========================================
// Multi-agent migration: Supervisor decision
// ========================================

type SupervisorAction string

const (
	SupervisorActionDelegate    SupervisorAction = "delegate"
	SupervisorActionAuthRequired SupervisorAction = "auth_required"
	SupervisorActionInputRequired SupervisorAction = "input_required"
	SupervisorActionCompleted   SupervisorAction = "completed"
	SupervisorActionFailed      SupervisorAction = "failed"
	SupervisorActionRejected    SupervisorAction = "rejected"
	SupervisorActionPlanReady   SupervisorAction = "plan_ready" // planner produced/refined a plan; flow continues
)

type SupervisorDecision struct {
	Action     SupervisorAction `json:"action"`
	AgentRole  AgentRole        `json:"agent_role,omitempty"`
	TodoID     string           `json:"todo_id,omitempty"`
	Payload    json.RawMessage  `json:"payload,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	MsgChainID int64            `json:"msg_chain_id,omitempty"`
	Message    string           `json:"message,omitempty"`
	Result     string           `json:"result,omitempty"`
	Error      string           `json:"error,omitempty"`
}

// ========================================
// Multi-agent migration: Structured error
// ========================================

type StructuredError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
	Agent   AgentRole `json:"agent,omitempty"`
}

func (e *StructuredError) Error() string {
	return e.Message
}

// ========================================
// Multi-agent migration: Todo status
// ========================================

type TodoStatus string

const (
	TodoStatusPending    TodoStatus = "pending"
	TodoStatusInProgress TodoStatus = "in_progress"
	TodoStatusCompleted  TodoStatus = "completed"
	TodoStatusFailed     TodoStatus = "failed"
	TodoStatusSkipped    TodoStatus = "skipped"
	TodoStatusBlocked    TodoStatus = "blocked"
)

// ========================================
// Multi-agent migration: Shared state patch
// ========================================

type SharedStatePatch struct {
	ActiveNode    string                 `json:"active_node,omitempty"`
	ActiveTodoID  string                 `json:"active_todo_id,omitempty"`
	TaskStatusCode *TaskStateCode        `json:"task_status_code,omitempty"`
	Updates       map[string]interface{} `json:"updates,omitempty"`
}

// ========================================
// Multi-agent migration: Agent state entry for logging
// ========================================

type AgentStateEntry struct {
	AgentRole AgentRole `json:"agent_role"`
	Status    string    `json:"status"`
	Timestamp string    `json:"timestamp"`
	Message   string    `json:"message,omitempty"`
}
