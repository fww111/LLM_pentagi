-- Multi-agent migration: Phase 0 database schema changes
-- Replaces subtask-based orchestration with todo-based multi-agent system

-- +goose Up
-- +goose StatementBegin

-- ========================================
-- enums: register multi-agent planner runtime types
-- ========================================
ALTER TYPE MSGCHAIN_TYPE ADD VALUE IF NOT EXISTS 'planner';
ALTER TYPE PROMPT_TYPE ADD VALUE IF NOT EXISTS 'planner';
ALTER TYPE PROMPT_TYPE ADD VALUE IF NOT EXISTS 'question_planner';

-- ========================================
-- tasks table: add new columns for multi-agent state management
-- ========================================
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS context_id VARCHAR(128);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS state_id VARCHAR(128);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS protocol_version VARCHAR(32) DEFAULT 'multi-agent-v1.0';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS shared_state JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS task_status_code INT DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS scope_contract JSONB;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS normalized_state VARCHAR(32) DEFAULT 'SUBMITTED';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS active_node VARCHAR(64);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS active_todo_id VARCHAR(128);

-- ========================================
-- todos table: replaces subtasks for the new multi-agent system
-- ========================================
CREATE TABLE IF NOT EXISTS todos (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128) NOT NULL,
    title TEXT NOT NULL,
    owner_agent VARCHAR(64) NOT NULL,
    depends_on JSONB DEFAULT '[]',
    need_env BOOLEAN DEFAULT FALSE,
    need_code BOOLEAN DEFAULT FALSE,
    risk_level VARCHAR(16) DEFAULT 'low',
    auth_required BOOLEAN DEFAULT FALSE,
    inputs TEXT,
    success_criteria TEXT,
    evidence_requirements JSONB DEFAULT '[]',
    data JSONB,
    todo_status_code INT DEFAULT 0,
    status VARCHAR(32) DEFAULT 'pending',
    result TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(task_id, todo_id)
);

-- ========================================
-- artifacts table: tracks task outputs and deliverables
-- ========================================
CREATE TABLE IF NOT EXISTS artifacts (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    artifact_id VARCHAR(128) NOT NULL,
    name TEXT NOT NULL,
    artifact_type VARCHAR(16) NOT NULL,
    relative_path TEXT,
    description TEXT,
    producer_agent VARCHAR(64),
    version VARCHAR(32),
    checksum VARCHAR(128),
    text TEXT,
    code_status JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(task_id, artifact_id)
);

-- ========================================
-- auth_requests table: authorization tracking for high-risk operations
-- ========================================
CREATE TABLE IF NOT EXISTS auth_requests (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    context_id VARCHAR(128) NOT NULL,
    todo_id VARCHAR(128),
    action TEXT NOT NULL,
    risk_level VARCHAR(16) NOT NULL,
    justification TEXT NOT NULL,
    status VARCHAR(32) DEFAULT 'pending',
    response TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- ========================================
-- findings table: pentester discoveries
-- ========================================
CREATE TABLE IF NOT EXISTS findings (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128),
    finding_type VARCHAR(64),
    severity VARCHAR(16),
    title TEXT NOT NULL,
    description TEXT,
    evidence JSONB DEFAULT '[]',
    raw_output TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ========================================
-- evidence table: evidence chain for audit
-- ========================================
CREATE TABLE IF NOT EXISTS evidence (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    todo_id VARCHAR(128),
    artifact_id VARCHAR(128),
    evidence_type VARCHAR(64),
    relative_path TEXT,
    description TEXT,
    hash VARCHAR(128),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ========================================
-- msglogs: add context_id column for message isolation
-- ========================================
ALTER TABLE msglogs ADD COLUMN IF NOT EXISTS context_id VARCHAR(128);

-- ========================================
-- Indexes for performance
-- ========================================
CREATE INDEX IF NOT EXISTS idx_todos_task_id ON todos(task_id);
CREATE INDEX IF NOT EXISTS idx_todos_status ON todos(task_id, status);
CREATE INDEX IF NOT EXISTS idx_artifacts_task_id ON artifacts(task_id);
CREATE INDEX IF NOT EXISTS idx_auth_requests_task_id ON auth_requests(task_id);
CREATE INDEX IF NOT EXISTS idx_auth_requests_status ON auth_requests(task_id, status);
CREATE INDEX IF NOT EXISTS idx_findings_task_id ON findings(task_id);
CREATE INDEX IF NOT EXISTS idx_evidence_task_id ON evidence(task_id);

-- +goose StatementEnd
