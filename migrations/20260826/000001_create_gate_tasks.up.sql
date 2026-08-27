CREATE TABLE IF NOT EXISTS keelmesh_gate_tasks (
    task_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    result_content TEXT NOT NULL DEFAULT '',
    progress_content TEXT NOT NULL DEFAULT '',
    progress_state TEXT NOT NULL DEFAULT '',
    last_sequence BIGINT NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 0,
    max_wall_time_ms BIGINT NOT NULL DEFAULT 0,
    require_approval BOOLEAN NOT NULL DEFAULT FALSE,
    approval_granted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS keelmesh_gate_tasks_state_idx
    ON keelmesh_gate_tasks (state, updated_at);
