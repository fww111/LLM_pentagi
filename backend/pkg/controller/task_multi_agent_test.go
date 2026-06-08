package controller

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"pentagi/pkg/database"
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
