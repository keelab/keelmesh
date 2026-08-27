ALTER TABLE keelmesh_gate_tasks
    DROP COLUMN IF EXISTS last_sequence,
    DROP COLUMN IF EXISTS last_event_id;
