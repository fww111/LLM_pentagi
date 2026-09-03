package providers

import (
	"testing"

	"pentagentx/pkg/tools"

	"github.com/sirupsen/logrus"
)

func currentTodosForPatch() []tools.TodoItem {
	return []tools.TodoItem{
		{TodoID: "todo_001", Title: "recon", OwnerAgent: "searcher", Status: "done"},
		{TodoID: "todo_002", Title: "exploit", OwnerAgent: "pentester", Status: "pending"},
	}
}

func TestValidateTodoPatchModifyWithoutTodoItemIsTolerated(t *testing.T) {
	patch := tools.TodoPatchAction{
		Operations: []tools.TodoOperation{
			{Op: tools.TodoOpModify, TodoID: "todo_002"}, // missing todo_item: must not fail
		},
	}
	if err := ValidateTodoPatch(patch); err != nil {
		t.Fatalf("ValidateTodoPatch() should tolerate modify without todo_item, got: %v", err)
	}
}

func TestValidateTodoPatchModifyWithoutTodoIDStillFails(t *testing.T) {
	patch := tools.TodoPatchAction{
		Operations: []tools.TodoOperation{
			{Op: tools.TodoOpModify, TodoItem: &tools.TodoItem{Title: "x"}},
		},
	}
	if err := ValidateTodoPatch(patch); err == nil {
		t.Fatal("ValidateTodoPatch() should reject modify without todo_id")
	}
}

func TestApplyTodoOperationsSkipsModifyWithoutTodoItem(t *testing.T) {
	patch := tools.TodoPatchAction{
		Operations: []tools.TodoOperation{
			{Op: tools.TodoOpModify, TodoID: "todo_001"}, // skipped, keeps "done"
			{Op: tools.TodoOpModify, TodoID: "todo_002", TodoItem: &tools.TodoItem{Status: "in_progress"}},
		},
	}
	result, err := ApplyTodoOperations(currentTodosForPatch(), patch, logrus.NewEntry(logrus.New()))
	if err != nil {
		t.Fatalf("ApplyTodoOperations() error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 todos, got %d", len(result))
	}
	if result[0].Status != "done" {
		t.Errorf("todo_001 should keep original status, got %q", result[0].Status)
	}
	if result[1].Status != "in_progress" {
		t.Errorf("todo_002 should be modified to in_progress, got %q", result[1].Status)
	}
}

func TestApplyTodoOperationsModifyUnknownTodoIDSkipped(t *testing.T) {
	patch := tools.TodoPatchAction{
		Operations: []tools.TodoOperation{
			{Op: tools.TodoOpModify, TodoID: "todo_999", TodoItem: &tools.TodoItem{Status: "done"}},
		},
	}
	result, err := ApplyTodoOperations(currentTodosForPatch(), patch, logrus.NewEntry(logrus.New()))
	if err != nil {
		t.Fatalf("ApplyTodoOperations() error: %v", err)
	}
	if len(result) != 2 || result[1].Status != "pending" {
		t.Errorf("unknown todo_id modify should be skipped without changes, got %+v", result)
	}
}

func TestApplyTodoOperationsAddStillValidated(t *testing.T) {
	patch := tools.TodoPatchAction{
		Operations: []tools.TodoOperation{
			{Op: tools.TodoOpAdd}, // missing todo_item: hard error remains
		},
	}
	if _, err := ApplyTodoOperations(currentTodosForPatch(), patch, logrus.NewEntry(logrus.New())); err == nil {
		t.Fatal("add without todo_item should still fail")
	}
}
