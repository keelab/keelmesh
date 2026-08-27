CREATE TABLE IF NOT EXISTS keelmesh_gate_audit_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    action TEXT NOT NULL,
    from_state TEXT NOT NULL DEFAULT '',
    to_state TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS keelmesh_gate_audit_task_idx
    ON keelmesh_gate_audit_events (task_id, occurred_at);
