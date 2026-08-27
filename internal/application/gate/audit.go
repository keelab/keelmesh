package gate

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type AuditEvent struct {
	ID         string
	TaskID     string
	Action     string
	FromState  string
	ToState    string
	Reason     string
	OccurredAt time.Time
}

type AuditStore interface {
	Append(context.Context, AuditEvent) error
	List(context.Context, string, int) ([]AuditEvent, error)
}

type memoryAuditStore struct {
	mu     sync.RWMutex
	events map[string][]AuditEvent
}

func NewMemoryAuditStore() AuditStore {
	return &memoryAuditStore{events: make(map[string][]AuditEvent)}
}

func (s *memoryAuditStore) Append(_ context.Context, event AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	s.events[event.TaskID] = append(s.events[event.TaskID], event)
	return nil
}

func (s *memoryAuditStore) List(_ context.Context, taskID string, limit int) ([]AuditEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := append([]AuditEvent(nil), s.events[taskID]...)
	if limit > 0 && len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items, nil
}

type PostgresAuditStore struct{ db *sql.DB }

func NewPostgresAuditStore(db *sql.DB) (*PostgresAuditStore, error) {
	if db == nil {
		return nil, fmt.Errorf("gatecore: audit database is nil")
	}
	return &PostgresAuditStore{db: db}, nil
}

func (s *PostgresAuditStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
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
    ON keelmesh_gate_audit_events (task_id, occurred_at)`)
	if err != nil {
		return fmt.Errorf("gatecore: ensure audit schema: %w", err)
	}
	return nil
}

func (s *PostgresAuditStore) Append(ctx context.Context, event AuditEvent) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO keelmesh_gate_audit_events
(event_id, task_id, action, from_state, to_state, reason, occurred_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)`, event.ID, event.TaskID, event.Action,
		event.FromState, event.ToState, event.Reason, event.OccurredAt)
	if err != nil {
		return fmt.Errorf("gatecore: append audit event: %w", err)
	}
	return nil
}

func (s *PostgresAuditStore) List(ctx context.Context, taskID string, limit int) ([]AuditEvent, error) {
	query := `SELECT event_id, action, from_state, to_state, reason, occurred_at
FROM keelmesh_gate_audit_events WHERE task_id = $1 ORDER BY occurred_at`
	args := []any{taskID}
	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("gatecore: list audit events: %w", err)
	}
	defer rows.Close()
	result := make([]AuditEvent, 0)
	for rows.Next() {
		var event AuditEvent
		if err := rows.Scan(&event.ID, &event.Action, &event.FromState, &event.ToState, &event.Reason, &event.OccurredAt); err != nil {
			return nil, fmt.Errorf("gatecore: scan audit event: %w", err)
		}
		event.TaskID = taskID
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gatecore: iterate audit events: %w", err)
	}
	return result, nil
}
