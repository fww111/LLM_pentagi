package providers

import (
	"fmt"
	"slices"

	"pentagentx/pkg/tools"

	"github.com/sirupsen/logrus"
)

func ApplyTodoOperations(
	current []tools.TodoItem,
	patch tools.TodoPatchAction,
	logger *logrus.Entry,
) ([]tools.TodoItem, error) {
	logger.WithFields(logrus.Fields{
		"current_count":    len(current),
		"operations_count": len(patch.Operations),
		"message":          patch.Message,
	}).Debug("applying todo operations")

	if err := ValidateTodoPatch(patch); err != nil {
		return nil, err
	}

	result := make([]tools.TodoItem, len(current))
	copy(result, current)
	idToIdx := buildTodoIndexMap(result)
	removed := make(map[string]bool)

	for _, op := range patch.Operations {
		switch op.Op {
		case tools.TodoOpRemove:
			if _, ok := idToIdx[op.TodoID]; !ok {
				continue
			}
			removed[op.TodoID] = true
		case tools.TodoOpModify:
			idx, ok := idToIdx[op.TodoID]
			if !ok {
				logger.WithField("todo_id", op.TodoID).Warn("todo modify references unknown todo_id, skipping")
				continue
			}
			if op.TodoItem == nil {
				logger.WithField("todo_id", op.TodoID).Warn("todo modify without todo_item, skipping")
				continue
			}
			updated := result[idx]
			mergeTodoItem(&updated, *op.TodoItem)
			updated.TodoID = op.TodoID
			result[idx] = updated
		}
	}

	if len(removed) > 0 {
		filtered := make([]tools.TodoItem, 0, len(result)-len(removed))
		for _, todo := range result {
			if !removed[todo.TodoID] {
				filtered = append(filtered, todo)
			}
		}
		result = filtered
	}

	idToIdx = buildTodoIndexMap(result)
	for i, op := range patch.Operations {
		switch op.Op {
		case tools.TodoOpAdd:
			if op.TodoItem == nil {
				return nil, fmt.Errorf("operation %d: add requires todo_item", i)
			}
			todo := *op.TodoItem
			if todo.TodoID == "" {
				todo.TodoID = nextTodoID(result)
			}
			if _, exists := idToIdx[todo.TodoID]; exists {
				todo.TodoID = nextTodoID(result)
			}
			normalizeTodoItem(&todo, len(result)+1)
			insertIdx := calculateTodoInsertIndex(op.AfterTodoID, idToIdx, len(result))
			result = slices.Insert(result, insertIdx, todo)
			idToIdx = buildTodoIndexMap(result)
		case tools.TodoOpReorder:
			currentIdx, ok := idToIdx[op.TodoID]
			if !ok {
				continue
			}
			todo := result[currentIdx]
			result = slices.Delete(result, currentIdx, currentIdx+1)
			idToIdx = buildTodoIndexMap(result)
			insertIdx := calculateTodoInsertIndex(op.AfterTodoID, idToIdx, len(result))
			result = slices.Insert(result, insertIdx, todo)
			idToIdx = buildTodoIndexMap(result)
		}
	}

	for i := range result {
		normalizeTodoItem(&result[i], i+1)
	}

	return result, nil
}

func ValidateTodoPatch(patch tools.TodoPatchAction) error {
	for i, op := range patch.Operations {
		switch op.Op {
		case tools.TodoOpAdd:
			if op.TodoItem == nil {
				return fmt.Errorf("operation %d: add requires todo_item", i)
			}
			if op.TodoItem.Title == "" {
				return fmt.Errorf("operation %d: add requires todo_item.title", i)
			}
			if op.TodoItem.OwnerAgent == "" {
				return fmt.Errorf("operation %d: add requires todo_item.owner_agent", i)
			}
		case tools.TodoOpRemove, tools.TodoOpReorder:
			if op.TodoID == "" {
				return fmt.Errorf("operation %d: %s requires todo_id", i, op.Op)
			}
		case tools.TodoOpModify:
			if op.TodoID == "" {
				return fmt.Errorf("operation %d: modify requires todo_id", i)
			}
			if op.TodoItem == nil {
				// LLMs occasionally omit todo_item on modify; the operation is
				// skipped by ApplyTodoOperations instead of failing the whole task.
				continue
			}
		default:
			return fmt.Errorf("operation %d: unknown operation type %q", i, op.Op)
		}
	}
	return nil
}

func normalizeTodoItems(todos []tools.TodoItem) []tools.TodoItem {
	normalized := make([]tools.TodoItem, 0, len(todos))
	seen := make(map[string]struct{}, len(todos))
	for i, todo := range todos {
		normalizeTodoItem(&todo, i+1)
		if _, ok := seen[todo.TodoID]; ok {
			todo.TodoID = nextTodoID(normalized)
		}
		seen[todo.TodoID] = struct{}{}
		normalized = append(normalized, todo)
	}
	return normalized
}

func normalizeTodoItem(todo *tools.TodoItem, position int) {
	if todo.TodoID == "" {
		todo.TodoID = fmt.Sprintf("todo_%03d", position)
	}
	if todo.OwnerAgent == "" {
		todo.OwnerAgent = "pentester"
	}
	if todo.RiskLevel == "" {
		todo.RiskLevel = "low"
	}
	if todo.Status == "" {
		todo.Status = "pending"
	}
}

func mergeTodoItem(dst *tools.TodoItem, src tools.TodoItem) {
	if src.Title != "" {
		dst.Title = src.Title
	}
	if src.OwnerAgent != "" {
		dst.OwnerAgent = src.OwnerAgent
	}
	if src.DependsOn != nil {
		dst.DependsOn = src.DependsOn
	}
	dst.NeedEnv = src.NeedEnv
	dst.NeedCode = src.NeedCode
	if src.RiskLevel != "" {
		dst.RiskLevel = src.RiskLevel
	}
	dst.AuthRequired = src.AuthRequired
	if src.Inputs != "" {
		dst.Inputs = src.Inputs
	}
	if src.SuccessCriteria != "" {
		dst.SuccessCriteria = src.SuccessCriteria
	}
	if src.EvidenceRequirements != nil {
		dst.EvidenceRequirements = src.EvidenceRequirements
	}
	if src.Status != "" {
		dst.Status = src.Status
	}
}

func buildTodoIndexMap(todos []tools.TodoItem) map[string]int {
	idToIdx := make(map[string]int, len(todos))
	for i, todo := range todos {
		if todo.TodoID != "" {
			idToIdx[todo.TodoID] = i
		}
	}
	return idToIdx
}

func calculateTodoInsertIndex(afterTodoID string, idToIdx map[string]int, length int) int {
	if afterTodoID == "" {
		return 0
	}
	if idx, ok := idToIdx[afterTodoID]; ok {
		return idx + 1
	}
	return length
}

func nextTodoID(todos []tools.TodoItem) string {
	seen := make(map[string]struct{}, len(todos))
	for _, todo := range todos {
		seen[todo.TodoID] = struct{}{}
	}
	for i := 1; ; i++ {
		id := fmt.Sprintf("todo_%03d", i)
		if _, ok := seen[id]; !ok {
			return id
		}
	}
}
