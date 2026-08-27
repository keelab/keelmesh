CREATE TABLE IF NOT EXISTS keelmesh_loop_runs (
    run_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    input TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL,
    current_step_id TEXT NOT NULL DEFAULT '',
    iteration INTEGER NOT NULL DEFAULT 0,
    output TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    steps JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS keelmesh_loop_runs_task_idx
    ON keelmesh_loop_runs (task_id, updated_at);
