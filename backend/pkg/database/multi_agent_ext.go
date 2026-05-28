package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// ========================================
// Multi-agent extension: manual DB operations for new tables
// These will be replaced by sqlc-generated code in a future cleanup pass.
// ========================================

// Todo represents a todo item in the multi-agent system
type Todo struct {
	ID                  int64            `json:"id"`
	TaskID              int64            `json:"task_id"`
	TodoID              string           `json:"todo_id"`
	Title               string           `json:"title"`
	OwnerAgent          string           `json:"owner_agent"`
	DependsOn           json.RawMessage  `json:"depends_on"`
	NeedEnv             bool             `json:"need_env"`
	NeedCode            bool             `json:"need_code"`
	RiskLevel           string           `json:"risk_level"`
	AuthRequired        bool             `json:"auth_required"`
	Inputs              sql.NullString   `json:"inputs"`
	SuccessCriteria     sql.NullString   `json:"success_criteria"`
	EvidenceRequirements json.RawMessage  `json:"evidence_requirements"`
	Data                json.RawMessage  `json:"data"`
	TodoStatusCode      int              `json:"todo_status_code"`
	Status              string           `json:"status"`
	Result              sql.NullString   `json:"result"`
	CreatedAt           sql.NullTime     `json:"created_at"`
	UpdatedAt           sql.NullTime     `json:"updated_at"`
}

// Artifact represents a task output artifact
type Artifact struct {
	ID            int64           `json:"id"`
	TaskID        int64           `json:"task_id"`
	ArtifactID    string          `json:"artifact_id"`
	Name          string          `json:"name"`
	ArtifactType  string          `json:"artifact_type"`
	RelativePath  sql.NullString  `json:"relative_path"`
	Description   sql.NullString  `json:"description"`
	ProducerAgent sql.NullString  `json:"producer_agent"`
	Version       sql.NullString  `json:"version"`
	Checksum      sql.NullString  `json:"checksum"`
	Text          sql.NullString  `json:"text"`
	CodeStatus    json.RawMessage `json:"code_status"`
	CreatedAt     sql.NullTime    `json:"created_at"`
}

// AuthRequest represents an authorization request for high-risk operations
type AuthRequest struct {
	ID           int64          `json:"id"`
	TaskID       int64          `json:"task_id"`
	ContextID    string         `json:"context_id"`
	TodoID       sql.NullString `json:"todo_id"`
	Action       string         `json:"action"`
	RiskLevel    string         `json:"risk_level"`
	Justification string        `json:"justification"`
	Status       string         `json:"status"`
	Response     sql.NullString `json:"response"`
	CreatedAt    sql.NullTime   `json:"created_at"`
	ResolvedAt   sql.NullTime   `json:"resolved_at"`
}

// Finding represents a pentester discovery
type Finding struct {
	ID          int64           `json:"id"`
	TaskID      int64           `json:"task_id"`
	TodoID      sql.NullString  `json:"todo_id"`
	FindingType sql.NullString  `json:"finding_type"`
	Severity    sql.NullString  `json:"severity"`
	Title       string          `json:"title"`
	Description sql.NullString  `json:"description"`
	Evidence    json.RawMessage `json:"evidence"`
	RawOutput   sql.NullString  `json:"raw_output"`
	CreatedAt   sql.NullTime    `json:"created_at"`
}

// Evidence represents an evidence chain entry for audit
type Evidence struct {
	ID           int64          `json:"id"`
	TaskID       int64          `json:"task_id"`
	TodoID       sql.NullString `json:"todo_id"`
	ArtifactID   sql.NullString `json:"artifact_id"`
	EvidenceType sql.NullString `json:"evidence_type"`
	RelativePath sql.NullString `json:"relative_path"`
	Description  sql.NullString `json:"description"`
	Hash         sql.NullString `json:"hash"`
	CreatedAt    sql.NullTime   `json:"created_at"`
}

// TaskExt holds the multi-agent extension fields for the tasks table
type TaskExt struct {
	ContextID       sql.NullString  `json:"context_id"`
	StateID         sql.NullString  `json:"state_id"`
	ProtocolVersion string          `json:"protocol_version"`
	SharedState     json.RawMessage `json:"shared_state"`
	TaskStatusCode  int             `json:"task_status_code"`
	ScopeContract   json.RawMessage `json:"scope_contract"`
	NormalizedState string          `json:"normalized_state"`
	ActiveNode      sql.NullString  `json:"active_node"`
	ActiveTodoID    sql.NullString  `json:"active_todo_id"`
}

// MultiAgentQueries provides database operations for the multi-agent system
type MultiAgentQueries struct {
	db DBTX
}

// NewMultiAgentQueries creates a new MultiAgentQueries instance
func NewMultiAgentQueries(db DBTX) *MultiAgentQueries {
	return &MultiAgentQueries{db: db}
}

// InsertTodo creates a new todo item
func (q *MultiAgentQueries) InsertTodo(ctx context.Context, todo *Todo) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO todos (task_id, todo_id, title, owner_agent, depends_on, need_env, need_code,
			risk_level, auth_required, inputs, success_criteria, evidence_requirements, data, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		todo.TaskID, todo.TodoID, todo.Title, todo.OwnerAgent, todo.DependsOn,
		todo.NeedEnv, todo.NeedCode, todo.RiskLevel, todo.AuthRequired,
		todo.Inputs, todo.SuccessCriteria, todo.EvidenceRequirements, todo.Data, todo.Status)
	return err
}

// GetTodosByTaskID retrieves all todos for a task
func (q *MultiAgentQueries) GetTodosByTaskID(ctx context.Context, taskID int64) ([]Todo, error) {
	rows, err := q.db.QueryContext(ctx,
		`SELECT id, task_id, todo_id, title, owner_agent, depends_on, need_env, need_code,
			risk_level, auth_required, inputs, success_criteria, evidence_requirements, data,
			todo_status_code, status, result, created_at, updated_at
		 FROM todos WHERE task_id = $1 ORDER BY id`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var todos []Todo
	for rows.Next() {
		var t Todo
		if err := rows.Scan(&t.ID, &t.TaskID, &t.TodoID, &t.Title, &t.OwnerAgent, &t.DependsOn,
			&t.NeedEnv, &t.NeedCode, &t.RiskLevel, &t.AuthRequired, &t.Inputs, &t.SuccessCriteria,
			&t.EvidenceRequirements, &t.Data, &t.TodoStatusCode, &t.Status, &t.Result,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		todos = append(todos, t)
	}
	return todos, rows.Err()
}

// UpdateTodoStatus updates the status of a todo item
func (q *MultiAgentQueries) UpdateTodoStatus(ctx context.Context, taskID int64, todoID, status, result string) error {
	_, err := q.db.ExecContext(ctx,
		`UPDATE todos SET status = $1, result = $2, updated_at = $3 WHERE task_id = $4 AND todo_id = $5`,
		status, result, time.Now(), taskID, todoID)
	return err
}

// UpsertArtifact inserts or updates an artifact
func (q *MultiAgentQueries) UpsertArtifact(ctx context.Context, a *Artifact) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO artifacts (task_id, artifact_id, name, artifact_type, relative_path, description,
			producer_agent, version, checksum, text, code_status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (task_id, artifact_id) DO UPDATE SET
			name = EXCLUDED.name, text = EXCLUDED.text, code_status = EXCLUDED.code_status,
			version = EXCLUDED.version`,
		a.TaskID, a.ArtifactID, a.Name, a.ArtifactType, a.RelativePath, a.Description,
		a.ProducerAgent, a.Version, a.Checksum, a.Text, a.CodeStatus)
	return err
}

// InsertAuthRequest creates a new authorization request
func (q *MultiAgentQueries) InsertAuthRequest(ctx context.Context, ar *AuthRequest) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO auth_requests (task_id, context_id, todo_id, action, risk_level, justification, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		ar.TaskID, ar.ContextID, ar.TodoID, ar.Action, ar.RiskLevel, ar.Justification, ar.Status)
	return err
}

// ResolveAuthRequest resolves an authorization request
func (q *MultiAgentQueries) ResolveAuthRequest(ctx context.Context, id int64, status, response string) error {
	_, err := q.db.ExecContext(ctx,
		`UPDATE auth_requests SET status = $1, response = $2, resolved_at = $3 WHERE id = $4`,
		status, response, time.Now(), id)
	return err
}

// InsertFinding creates a new finding
func (q *MultiAgentQueries) InsertFinding(ctx context.Context, f *Finding) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO findings (task_id, todo_id, finding_type, severity, title, description, evidence, raw_output)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		f.TaskID, f.TodoID, f.FindingType, f.Severity, f.Title, f.Description, f.Evidence, f.RawOutput)
	return err
}

// InsertEvidence creates a new evidence entry
func (q *MultiAgentQueries) InsertEvidence(ctx context.Context, e *Evidence) error {
	_, err := q.db.ExecContext(ctx,
		`INSERT INTO evidence (task_id, todo_id, artifact_id, evidence_type, relative_path, description, hash)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.TaskID, e.TodoID, e.ArtifactID, e.EvidenceType, e.RelativePath, e.Description, e.Hash)
	return err
}

// UpdateTaskExtension updates the multi-agent extension fields on a task
func (q *MultiAgentQueries) UpdateTaskExtension(ctx context.Context, taskID int64, ext *TaskExt) error {
	_, err := q.db.ExecContext(ctx,
		`UPDATE tasks SET context_id = $1, state_id = $2, shared_state = $3,
			task_status_code = $4, scope_contract = $5, normalized_state = $6,
			active_node = $7, active_todo_id = $8, updated_at = $9
		 WHERE id = $10`,
		ext.ContextID, ext.StateID, ext.SharedState, ext.TaskStatusCode,
		ext.ScopeContract, ext.NormalizedState, ext.ActiveNode, ext.ActiveTodoID,
		time.Now(), taskID)
	return err
}

// GetTaskExtension retrieves the multi-agent extension fields for a task
func (q *MultiAgentQueries) GetTaskExtension(ctx context.Context, taskID int64) (*TaskExt, error) {
	var ext TaskExt
	err := q.db.QueryRowContext(ctx,
		`SELECT context_id, state_id, COALESCE(protocol_version, ''), shared_state, task_status_code,
			scope_contract, COALESCE(normalized_state, 'SUBMITTED'), active_node, active_todo_id
		 FROM tasks WHERE id = $1`, taskID).
		Scan(&ext.ContextID, &ext.StateID, &ext.ProtocolVersion, &ext.SharedState,
			&ext.TaskStatusCode, &ext.ScopeContract, &ext.NormalizedState,
			&ext.ActiveNode, &ext.ActiveTodoID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task extension: %w", err)
	}
	return &ext, nil
}

// DeleteTodosByTaskID removes all todos for a task (used when regenerating plan)
func (q *MultiAgentQueries) DeleteTodosByTaskID(ctx context.Context, taskID int64) error {
	_, err := q.db.ExecContext(ctx, `DELETE FROM todos WHERE task_id = $1`, taskID)
	return err
}

// BatchInsertTodos creates multiple todo items in a single transaction
func (q *MultiAgentQueries) BatchInsertTodos(ctx context.Context, todos []Todo) error {
	tx, ok := q.db.(*sql.Tx)
	if !ok {
		var err error
		tx, err = q.db.(*sql.DB).BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()
	}

	for _, todo := range todos {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO todos (task_id, todo_id, title, owner_agent, depends_on, need_env, need_code,
				risk_level, auth_required, inputs, success_criteria, evidence_requirements, data, status)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			todo.TaskID, todo.TodoID, todo.Title, todo.OwnerAgent, todo.DependsOn,
			todo.NeedEnv, todo.NeedCode, todo.RiskLevel, todo.AuthRequired,
			todo.Inputs, todo.SuccessCriteria, todo.EvidenceRequirements, todo.Data, todo.Status)
		if err != nil {
			return fmt.Errorf("failed to insert todo %s: %w", todo.TodoID, err)
		}
	}

	return tx.Commit()
}
