package gate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PostgresTaskRepository stores Gate task state durably. The table is kept
// deliberately small because task payloads are already normalized at the
// Gate boundary; progress/result fields are bounded by the caller contract.
type PostgresTaskRepository struct {
	db *sql.DB
}

func (r *PostgresTaskRepository) CreateWithAudit(ctx context.Context, id string, record TaskRecord, event AuditEvent) error {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("gatecore: encode task %q metadata: %w", id, err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gatecore: begin task creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `INSERT INTO keelmesh_gate_tasks
(task_id, channel_id, target_id, content, metadata, state, error, result_content,
progress_content, progress_state, last_sequence, last_event_id, attempt,
max_attempts, max_wall_time_ms, require_approval, approval_granted)
VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`, id,
		record.ChannelID, record.TargetID, record.Content, string(metadata), record.State,
		record.Error, record.Result, record.Progress, record.ProgressState, record.LastSequence,
		record.LastEventID, record.Attempt, record.MaxAttempts, record.MaxWallTimeMS,
		record.RequireApproval, record.ApprovalGranted); err != nil {
		return fmt.Errorf("gatecore: create task: %w", err)
	}
	if err := insertAuditTx(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gatecore: commit task creation: %w", err)
	}
	return nil
}

func (r *PostgresTaskRepository) TransitionWithAudit(ctx context.Context, id, expectedState string, record TaskRecord, event AuditEvent) (bool, error) {
	return r.transitionWithAudit(ctx, id, expectedState, record, event, nil)
}

func (r *PostgresTaskRepository) TransitionWithAuditAndOutbox(ctx context.Context, id, expectedState string, record TaskRecord, event AuditEvent, enqueue func(context.Context, *sql.Tx) error) (bool, error) {
	return r.transitionWithAudit(ctx, id, expectedState, record, event, enqueue)
}

func (r *PostgresTaskRepository) transitionWithAudit(ctx context.Context, id, expectedState string, record TaskRecord, event AuditEvent, enqueue func(context.Context, *sql.Tx) error) (bool, error) {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return false, fmt.Errorf("gatecore: encode transition task %q metadata: %w", id, err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("gatecore: begin audited transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, id); err != nil {
		return false, fmt.Errorf("gatecore: lock audited transition: %w", err)
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM keelmesh_gate_tasks WHERE task_id=$1`, id).Scan(&state); err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("gatecore: read audited transition state: %w", err)
	}
	if state != expectedState {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE keelmesh_gate_tasks SET channel_id=$2,target_id=$3,content=$4,metadata=$5::jsonb,state=$6,error=$7,result_content=$8,progress_content=$9,progress_state=$10,last_sequence=$11,last_event_id=$12,attempt=$13,max_attempts=$14,max_wall_time_ms=$15,require_approval=$16,approval_granted=$17,updated_at=now() WHERE task_id=$1`, id, record.ChannelID, record.TargetID, record.Content, string(metadata), record.State, record.Error, record.Result, record.Progress, record.ProgressState, record.LastSequence, record.LastEventID, record.Attempt, record.MaxAttempts, record.MaxWallTimeMS, record.RequireApproval, record.ApprovalGranted); err != nil {
		return false, fmt.Errorf("gatecore: write audited transition: %w", err)
	}
	if err := insertAuditTx(ctx, tx, event); err != nil {
		return false, err
	}
	if enqueue != nil {
		if err := enqueue(ctx, tx); err != nil {
			return false, fmt.Errorf("gatecore: enqueue transition delivery: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("gatecore: commit audited transition: %w", err)
	}
	return true, nil
}

func insertAuditTx(ctx context.Context, tx *sql.Tx, event AuditEvent) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO keelmesh_gate_audit_events
(event_id, task_id, action, from_state, to_state, reason, occurred_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)`, event.ID, event.TaskID, event.Action, event.FromState, event.ToState, event.Reason, event.OccurredAt); err != nil {
		return fmt.Errorf("gatecore: insert audit in task transaction: %w", err)
	}
	return nil
}

func (r *PostgresTaskRepository) Transition(ctx context.Context, id, expectedState string, record TaskRecord) (bool, error) {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return false, fmt.Errorf("gatecore: encode transition task %q metadata: %w", id, err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("gatecore: begin task transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, id); err != nil {
		return false, fmt.Errorf("gatecore: lock task transition: %w", err)
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM keelmesh_gate_tasks WHERE task_id = $1`, id).Scan(&state); err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("gatecore: read task transition state: %w", err)
	}
	if state != expectedState {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE keelmesh_gate_tasks SET
channel_id=$2, target_id=$3, content=$4, metadata=$5::jsonb, state=$6, error=$7,
result_content=$8, progress_content=$9, progress_state=$10, last_sequence=$11,
last_event_id=$12, attempt=$13, max_attempts=$14, max_wall_time_ms=$15,
require_approval=$16, approval_granted=$17, updated_at=now() WHERE task_id=$1`,
		id, record.ChannelID, record.TargetID, record.Content, string(metadata), record.State,
		record.Error, record.Result, record.Progress, record.ProgressState, record.LastSequence,
		record.LastEventID, record.Attempt, record.MaxAttempts, record.MaxWallTimeMS,
		record.RequireApproval, record.ApprovalGranted)
	if err != nil {
		return false, fmt.Errorf("gatecore: write task transition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("gatecore: commit task transition: %w", err)
	}
	return true, nil
}

func NewPostgresTaskRepository(db *sql.DB) (*PostgresTaskRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("gatecore: postgres task repository database is nil")
	}
	return &PostgresTaskRepository{db: db}, nil
}

func (r *PostgresTaskRepository) EnsureSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
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
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 0,
    max_wall_time_ms BIGINT NOT NULL DEFAULT 0,
    require_approval BOOLEAN NOT NULL DEFAULT FALSE,
    approval_granted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	if err != nil {
		return fmt.Errorf("gatecore: ensure task schema: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, `
ALTER TABLE keelmesh_gate_tasks
    ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_wall_time_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS require_approval BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS approval_granted BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return fmt.Errorf("gatecore: ensure task governance columns: %w", err)
	}
	return nil
}

func (r *PostgresTaskRepository) Get(ctx context.Context, id string) (TaskRecord, bool, error) {
	var record TaskRecord
	var metadata []byte
	err := r.db.QueryRowContext(ctx, `
SELECT channel_id, target_id, content, metadata, state, error,
       result_content, progress_content, progress_state
       , last_sequence, last_event_id, attempt, max_attempts, max_wall_time_ms,
       require_approval, approval_granted
FROM keelmesh_gate_tasks WHERE task_id = $1`, id).Scan(
		&record.ChannelID, &record.TargetID, &record.Content, &metadata,
		&record.State, &record.Error, &record.Result, &record.Progress,
		&record.ProgressState, &record.LastSequence, &record.LastEventID,
		&record.Attempt, &record.MaxAttempts, &record.MaxWallTimeMS,
		&record.RequireApproval, &record.ApprovalGranted,
	)
	if err == sql.ErrNoRows {
		return TaskRecord{}, false, nil
	}
	if err != nil {
		return TaskRecord{}, false, fmt.Errorf("gatecore: get task %q: %w", id, err)
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &record.Metadata); err != nil {
			return TaskRecord{}, false, fmt.Errorf("gatecore: decode task %q metadata: %w", id, err)
		}
	}
	return record, true, nil
}

func (r *PostgresTaskRepository) Put(ctx context.Context, id string, record TaskRecord) error {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("gatecore: encode task %q metadata: %w", id, err)
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO keelmesh_gate_tasks
    (task_id, channel_id, target_id, content, metadata, state, error,
     result_content, progress_content, progress_state, last_sequence, last_event_id, attempt,
     max_attempts, max_wall_time_ms, require_approval, approval_granted)
VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (task_id) DO UPDATE SET
    channel_id = EXCLUDED.channel_id,
    target_id = EXCLUDED.target_id,
    content = EXCLUDED.content,
    metadata = EXCLUDED.metadata,
    state = EXCLUDED.state,
    error = EXCLUDED.error,
    result_content = EXCLUDED.result_content,
    progress_content = EXCLUDED.progress_content,
    progress_state = EXCLUDED.progress_state,
    last_sequence = EXCLUDED.last_sequence,
    last_event_id = EXCLUDED.last_event_id,
    attempt = EXCLUDED.attempt,
    max_attempts = EXCLUDED.max_attempts,
    max_wall_time_ms = EXCLUDED.max_wall_time_ms,
    require_approval = EXCLUDED.require_approval,
    approval_granted = EXCLUDED.approval_granted,
    updated_at = now()`, id, record.ChannelID, record.TargetID, record.Content,
		string(metadata), record.State, record.Error, record.Result, record.Progress,
		record.ProgressState, record.LastSequence, record.LastEventID, record.Attempt,
		record.MaxAttempts, record.MaxWallTimeMS, record.RequireApproval, record.ApprovalGranted)
	if err != nil {
		return fmt.Errorf("gatecore: put task %q: %w", id, err)
	}
	return nil
}

func (r *PostgresTaskRepository) ListByState(ctx context.Context, states ...string) ([]TaskEntry, error) {
	if len(states) == 0 {
		return nil, nil
	}
	args := make([]any, len(states))
	placeholders := make([]string, len(states))
	for i, state := range states {
		args[i] = state
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT task_id, channel_id, target_id, content, metadata, state, error, result_content, progress_content, progress_state, last_sequence, last_event_id, attempt, max_attempts, max_wall_time_ms, require_approval, approval_granted FROM keelmesh_gate_tasks WHERE state IN (`+strings.Join(placeholders, ",")+")", args...)
	if err != nil {
		return nil, fmt.Errorf("gatecore: list tasks by state: %w", err)
	}
	defer rows.Close()
	entries := make([]TaskEntry, 0)
	for rows.Next() {
		var id string
		var record TaskRecord
		var metadata []byte
		if err := rows.Scan(&id, &record.ChannelID, &record.TargetID, &record.Content, &metadata, &record.State, &record.Error, &record.Result, &record.Progress, &record.ProgressState, &record.LastSequence, &record.LastEventID, &record.Attempt, &record.MaxAttempts, &record.MaxWallTimeMS, &record.RequireApproval, &record.ApprovalGranted); err != nil {
			return nil, fmt.Errorf("gatecore: scan recoverable task: %w", err)
		}
		if err := json.Unmarshal(metadata, &record.Metadata); err != nil {
			return nil, fmt.Errorf("gatecore: decode recoverable task %q metadata: %w", id, err)
		}
		entries = append(entries, TaskEntry{ID: id, Record: record})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gatecore: iterate recoverable tasks: %w", err)
	}
	return entries, nil
}
