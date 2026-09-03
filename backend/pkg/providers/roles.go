package providers

import (
	"strings"

	"pentagentx/pkg/orchestrator"
	"pentagentx/pkg/tools"
)

// RoleSpec is the single source of truth for every agent role in the
// multi-agent topology. Every place that needs to translate between role
// names, decision tool names, message-chain types and execution handlers
// must derive that information from this registry instead of maintaining
// its own switch statement.
type RoleSpec struct {
	// Role is the canonical role name (matches orchestrator.AgentRole).
	Role orchestrator.AgentRole
	// Aliases are alternative spellings or legacy names that LLM output or
	// persisted plans may still use (e.g. installer, coder, searcher).
	Aliases []string
	// ToolName is the decision/result tool associated with the role.
	ToolName string
	// RouteToolName is the supervisor delegation tool (route_to_*).
	// Empty for pipeline nodes the supervisor cannot route back to.
	RouteToolName string
	// HandlerKey selects the execution handler inside the providers package.
	HandlerKey string
	// StoresFinding marks roles whose execution results are persisted as findings.
	StoresFinding bool
	// Pipeline marks the fixed graph nodes (designer/planner/supervisor) as
	// opposed to delegatable worker agents.
	Pipeline bool
}

var agentRoleRegistry = map[orchestrator.AgentRole]RoleSpec{
	orchestrator.AgentRoleDesigner: {
		Role:       orchestrator.AgentRoleDesigner,
		ToolName:   tools.ScopeContractToolName,
		HandlerKey: "designer",
		Pipeline:   true,
	},
	orchestrator.AgentRolePlanner: {
		Role:          orchestrator.AgentRolePlanner,
		ToolName:      tools.TodoListToolName,
		RouteToolName: tools.RouteToPlannerToolName,
		HandlerKey:    "planner",
		Pipeline:      true,
	},
	orchestrator.AgentRoleSupervisor: {
		Role:       orchestrator.AgentRoleSupervisor,
		HandlerKey: "supervisor",
		Pipeline:   true,
	},
	orchestrator.AgentRoleBuilder: {
		Role:          orchestrator.AgentRoleBuilder,
		Aliases:       []string{"installer"},
		ToolName:      tools.MaintenanceToolName,
		RouteToolName: tools.RouteToBuilderToolName,
		HandlerKey:    "installer",
	},
	orchestrator.AgentRoleGenerator: {
		Role:          orchestrator.AgentRoleGenerator,
		Aliases:       []string{"coder", "code", "developer"},
		ToolName:      tools.CoderToolName,
		RouteToolName: tools.RouteToGeneratorToolName,
		HandlerKey:    "coder",
	},
	orchestrator.AgentRoleIntegrator: {
		Role:          orchestrator.AgentRoleIntegrator,
		ToolName:      tools.IntegratorToolName,
		RouteToolName: tools.RouteToIntegratorToolName,
		HandlerKey:    "integrator",
	},
	orchestrator.AgentRoleTester: {
		Role:          orchestrator.AgentRoleTester,
		ToolName:      tools.TesterToolName,
		RouteToolName: tools.RouteToTesterToolName,
		HandlerKey:    "tester",
		StoresFinding: true,
	},
	orchestrator.AgentRolePentester: {
		Role:          orchestrator.AgentRolePentester,
		Aliases:       []string{"pentest", "security_tester"},
		ToolName:      tools.PentesterToolName,
		RouteToolName: tools.RouteToPentesterToolName,
		HandlerKey:    "pentester",
		StoresFinding: true,
	},
	orchestrator.AgentRoleReviewer: {
		Role:          orchestrator.AgentRoleReviewer,
		ToolName:      tools.ReviewResultToolName,
		RouteToolName: tools.RouteToReviewerToolName,
		HandlerKey:    "reviewer",
		StoresFinding: true,
	},
	orchestrator.AgentRoleReporter: {
		Role:          orchestrator.AgentRoleReporter,
		ToolName:      tools.ReportResultToolName,
		RouteToolName: tools.RouteToReporterToolName,
		HandlerKey:    "reporter",
	},
	orchestrator.AgentRoleResearcher: {
		Role:          orchestrator.AgentRoleResearcher,
		Aliases:       []string{"searcher"},
		ToolName:      tools.SearchToolName,
		RouteToolName: tools.RouteToResearcherToolName,
		HandlerKey:    "searcher",
	},
	orchestrator.AgentRole("memorist"): {
		Role:       orchestrator.AgentRole("memorist"),
		ToolName:   tools.MemoristToolName,
		HandlerKey: "memorist",
	},
	orchestrator.AgentRole("adviser"): {
		Role:       orchestrator.AgentRole("adviser"),
		ToolName:   tools.AdviceToolName,
		HandlerKey: "adviser",
	},
}

// workerRoles returns the registry entries that the supervisor may delegate
// work to, in a stable order. Pipeline nodes (designer/planner/supervisor)
// are excluded: the topology is one-way (designer -> planner -> supervisor ->
// agents) and the supervisor never routes back to them.
func workerRoles() []RoleSpec {
	order := []orchestrator.AgentRole{
		orchestrator.AgentRolePlanner,
		orchestrator.AgentRoleBuilder,
		orchestrator.AgentRoleGenerator,
		orchestrator.AgentRoleIntegrator,
		orchestrator.AgentRoleTester,
		orchestrator.AgentRolePentester,
		orchestrator.AgentRoleReviewer,
		orchestrator.AgentRoleReporter,
		orchestrator.AgentRoleResearcher,
	}
	specs := make([]RoleSpec, 0, len(order))
	for _, role := range order {
		specs = append(specs, agentRoleRegistry[role])
	}
	return specs
}

// lookupRole resolves a raw string (canonical name or alias) to a RoleSpec.
func lookupRole(raw string) (RoleSpec, bool) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if name == "" {
		return RoleSpec{}, false
	}
	role := orchestrator.AgentRole(name)
	if spec, ok := agentRoleRegistry[role]; ok {
		return spec, true
	}
	for _, spec := range agentRoleRegistry {
		for _, alias := range spec.Aliases {
			if alias == name {
				return spec, true
			}
		}
	}
	return RoleSpec{}, false
}

// NormalizeRole resolves a raw role string (canonical name or legacy alias)
// to the canonical role name. Unknown values are returned lower-cased.
func NormalizeRole(raw string) string {
	if spec, ok := lookupRole(raw); ok {
		return string(spec.Role)
	}
	return strings.ToLower(strings.TrimSpace(raw))
}

// toolNameToAgentRole maps a decision tool name back to its role spec.
func toolNameToAgentRole(toolName string) (RoleSpec, error) {
	for _, spec := range agentRoleRegistry {
		if spec.ToolName != "" && spec.ToolName == toolName {
			return spec, nil
		}
	}
	return RoleSpec{}, &unknownToolError{ToolName: toolName}
}

type unknownToolError struct {
	ToolName string
}

func (e *unknownToolError) Error() string {
	return "unsupported agent tool " + e.ToolName
}
