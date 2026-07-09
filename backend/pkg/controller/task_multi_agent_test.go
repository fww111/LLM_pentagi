package controller

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"pentagi/pkg/database"
	"pentagi/pkg/tools"
)

func TestExtractTodoIDFromPayload(t *testing.T) {
	payload := json.RawMessage(`{
		"question": "Run delegated work. Active todo id: todo_003.",
		"metadata": {"active_todo_id": "todo_002"}
	}`)

	if got := extractTodoIDFromPayload(payload); got != "todo_002" {
		t.Fatalf("expected explicit active_todo_id, got %q", got)
	}

	payload = json.RawMessage(`{"question":"Active todo id: todo_004, execute now"}`)
	if got := extractTodoIDFromPayload(payload); got != "todo_004" {
		t.Fatalf("expected todo id scanned from text, got %q", got)
	}
}

func TestMultiAgentCompletionResultIncludesStructuredState(t *testing.T) {
	result := multiAgentCompletionResult(
		"MySQL service test",
		"test mysql weak credentials",
		[]database.Todo{{
			TodoID:     "todo_001",
			Title:      "Check weak credentials",
			OwnerAgent: "pentester",
			RiskLevel:  "high",
			Status:     "completed",
			Result:     sql.NullString{String: "root login succeeded", Valid: true},
		}},
		[]database.Finding{{
			TodoID:      sql.NullString{String: "todo_001", Valid: true},
			Severity:    sql.NullString{String: "high", Valid: true},
			Title:       "Weak MySQL root password",
			Description: sql.NullString{String: "root:root authenticated successfully", Valid: true},
		}},
		[]database.Evidence{{
			TodoID:       sql.NullString{String: "todo_001", Valid: true},
			EvidenceType: sql.NullString{String: "agent_result", Valid: true},
			Description:  sql.NullString{String: "mysql -uroot -proot succeeded", Valid: true},
		}},
	)

	for _, want := range []string{"## Todos", "todo_001 [completed]", "## Findings", "Weak MySQL root password", "## Evidence", "agent_result"} {
		if !strings.Contains(result, want) {
			t.Fatalf("expected completion result to contain %q, got:\n%s", want, result)
		}
	}
}

func TestMultiAgentCompletionStatusBlocksOpenTodos(t *testing.T) {
	status, err := multiAgentCompletionStatus([]database.Todo{
		{TaskID: 10, TodoID: "todo_001", Status: "completed"},
		{TaskID: 10, TodoID: "todo_002", Status: "pending"},
	})
	if err == nil {
		t.Fatalf("expected open todo to block completion")
	}
	if status != database.TaskStatusRunning {
		t.Fatalf("expected running status while completion is blocked, got %s", status)
	}
}

func TestMultiAgentCompletionStatusFailedWhenTodoFailed(t *testing.T) {
	status, err := multiAgentCompletionStatus([]database.Todo{
		{TaskID: 10, TodoID: "todo_001", Status: "completed"},
		{TaskID: 10, TodoID: "todo_002", Status: "failed"},
	})
	if err != nil {
		t.Fatalf("did not expect error for closed failed todos: %v", err)
	}
	if status != database.TaskStatusFailed {
		t.Fatalf("expected failed task status, got %s", status)
	}
}

func TestRepairCompletedTodosFromStructuredOutput(t *testing.T) {
	todos := []database.Todo{
		{TaskID: 10, TodoID: "todo_001", Title: "Query database version", Status: "in_progress"},
		{TaskID: 10, TodoID: "todo_002", Title: "Generate report", Status: "created"},
	}
	evidence := []database.Evidence{{
		TodoID:      sql.NullString{String: "todo_001", Valid: true},
		Description: sql.NullString{String: "version() returned 5.6.51", Valid: true},
	}}

	repaired := repairCompletedTodosFromStructuredOutput(todos, nil, evidence)
	if len(repaired) != 1 {
		t.Fatalf("expected one repaired todo, got %d", len(repaired))
	}
	if repaired[0].TodoID != "todo_001" || repaired[0].Status != "completed" {
		t.Fatalf("unexpected repaired todo: %+v", repaired[0])
	}
	if !repaired[0].Result.Valid || !strings.Contains(repaired[0].Result.String, "5.6.51") {
		t.Fatalf("expected repair to carry evidence summary, got %+v", repaired[0].Result)
	}
}

func TestMultiAgentCompletionStatusAcceptsClosedAliases(t *testing.T) {
	status, err := multiAgentCompletionStatus([]database.Todo{
		{TaskID: 10, TodoID: "todo_001", Status: "done"},
		{TaskID: 10, TodoID: "todo_002", Status: "skipped"},
	})
	if err != nil {
		t.Fatalf("did not expect closed aliases to block completion: %v", err)
	}
	if status != database.TaskStatusFinished {
		t.Fatalf("expected finished status, got %s", status)
	}
}

func TestTodoStatusClassificationPreventsClosedTodoReopen(t *testing.T) {
	for _, status := range []string{"completed", "done", "skipped", "success", "failed", "error", "rejected"} {
		if isOpenTodoStatus(status) {
			t.Fatalf("expected closed status %q to be non-open", status)
		}
	}
	for _, status := range []string{"pending", "in_progress", "blocked", "created", "waiting", "running", ""} {
		if !isOpenTodoStatus(status) {
			t.Fatalf("expected status %q to be open", status)
		}
	}
	for _, status := range []string{"failed", "error", "rejected"} {
		if !isFailedTodoStatus(status) {
			t.Fatalf("expected status %q to be failed", status)
		}
	}
	for _, status := range []string{"completed", "done", "skipped", "success"} {
		if isFailedTodoStatus(status) {
			t.Fatalf("did not expect status %q to be failed", status)
		}
	}
}

func TestShouldStoreFindingForSecurityRoles(t *testing.T) {
	for _, role := range []string{"pentester", "tester", "reviewer", "security_tester"} {
		if !shouldStoreFindingForRole(role) {
			t.Fatalf("expected %s result to be stored as finding", role)
		}
	}
	for _, role := range []string{"builder", "reporter", "researcher"} {
		if shouldStoreFindingForRole(role) {
			t.Fatalf("did not expect %s result to be stored as finding", role)
		}
	}
}

func TestSanitizeTodoPlanRewritesEnvironmentOnlySecurityPlan(t *testing.T) {
	input := "对本地 Redis 4.0 靶场进行授权安全测试，验证未授权访问风险，只使用只读命令，不要配置 Redis。"
	plan := []tools.TodoItem{{
		TodoID:       "todo_001",
		Title:        "环境准备：检查并配置 Redis 4.0 靶场环境",
		OwnerAgent:   "builder",
		NeedEnv:      true,
		RiskLevel:    "low",
		Inputs:       "配置 Redis 环境",
		Status:       "pending",
		AuthRequired: false,
	}}

	got := sanitizeTodoPlanForSecurityValidation(input, plan)
	if len(got) != 2 {
		t.Fatalf("expected validation todo plus reporter todo, got %d: %+v", len(got), got)
	}
	if got[0].OwnerAgent != "pentester" || got[0].NeedEnv || got[0].NeedCode {
		t.Fatalf("expected first todo to be non-env pentester validation, got %+v", got[0])
	}
	if got[1].OwnerAgent != "reporter" {
		t.Fatalf("expected reporter todo to be appended, got %+v", got[1])
	}
	if !strings.Contains(got[0].Inputs, "不要配置") {
		t.Fatalf("expected validation todo to preserve no-configuration constraint, got %q", got[0].Inputs)
	}
}

func TestSanitizeTodoPlanKeepsExplicitEnvironmentWork(t *testing.T) {
	input := "请先搭建靶场环境，然后进行授权安全测试。"
	plan := []tools.TodoItem{{
		TodoID:     "todo_001",
		Title:      "搭建测试环境",
		OwnerAgent: "builder",
		NeedEnv:    true,
		Status:     "pending",
	}}

	got := sanitizeTodoPlanForSecurityValidation(input, plan)
	foundBuilder := false
	for _, item := range got {
		if item.TodoID == "todo_001" && item.OwnerAgent == "builder" && item.NeedEnv {
			foundBuilder = true
		}
	}
	if !foundBuilder {
		t.Fatalf("expected explicit environment work to remain builder, got %+v", got)
	}
}
