CREATE TABLE IF NOT EXISTS keelmesh_agent_task_events (
    task_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, sequence),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS keelmesh_agent_task_events_task_idx
    ON keelmesh_agent_task_events (task_id, sequence);
