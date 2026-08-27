ALTER TABLE keelmesh_gate_tasks
    DROP COLUMN IF EXISTS approval_granted,
    DROP COLUMN IF EXISTS require_approval,
    DROP COLUMN IF EXISTS max_wall_time_ms,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS attempt;
