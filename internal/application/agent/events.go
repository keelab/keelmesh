package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
)

type EventStore interface {
	Append(context.Context, *agentv1.TaskEvent) error
	List(context.Context, string, int64) ([]*agentv1.TaskEvent, error)
}

type memoryEventStore struct {
	mu     sync.RWMutex
	byTask map[string][]*agentv1.TaskEvent
}

func NewMemoryEventStore() EventStore {
	return &memoryEventStore{byTask: make(map[string][]*agentv1.TaskEvent)}
}

func (s *memoryEventStore) Append(_ context.Context, event *agentv1.TaskEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.byTask[event.GetTaskId()]
	event.Sequence = int64(len(items) + 1)
	event.EventId = fmt.Sprintf("%s-%d", event.GetTaskId(), event.GetSequence())
	event.OccurredAtMs = time.Now().UnixMilli()
	event = cloneEvent(event)
	items = append(items, event)
	s.byTask[event.GetTaskId()] = items
	return nil
}

func (s *memoryEventStore) List(_ context.Context, taskID string, after int64) ([]*agentv1.TaskEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := s.byTask[taskID]
	result := make([]*agentv1.TaskEvent, 0, len(items))
	for _, event := range items {
		if event.GetSequence() > after {
			result = append(result, cloneEvent(event))
		}
	}
	return result, nil
}

type PostgresEventStore struct{ db *sql.DB }

func NewPostgresEventStore(db *sql.DB) (*PostgresEventStore, error) {
	if db == nil {
		return nil, fmt.Errorf("agentcore: event store database is nil")
	}
	return &PostgresEventStore{db: db}, nil
}

func (s *PostgresEventStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS keelmesh_agent_task_events (
    task_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, sequence),
    UNIQUE (event_id)
)`)
	if err != nil {
		return fmt.Errorf("agentcore: ensure event schema: %w", err)
	}
	return nil
}

func (s *PostgresEventStore) Append(ctx context.Context, event *agentv1.TaskEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agentcore: begin event transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var sequence int64
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, event.GetTaskId()); err != nil {
		return fmt.Errorf("agentcore: lock task event sequence: %w", err)
	}
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM keelmesh_agent_task_events WHERE task_id = $1`, event.GetTaskId()).Scan(&sequence)
	if err != nil {
		return fmt.Errorf("agentcore: allocate task event sequence: %w", err)
	}
	event.Sequence = sequence
	event.EventId = fmt.Sprintf("%s-%d", event.GetTaskId(), sequence)
	event.OccurredAtMs = time.Now().UnixMilli()
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("agentcore: encode task event: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO keelmesh_agent_task_events(task_id, sequence, event_id, state, payload, occurred_at) VALUES($1,$2,$3,$4,$5::jsonb,now())`, event.GetTaskId(), sequence, event.GetEventId(), event.GetState(), string(payload))
	if err != nil {
		return fmt.Errorf("agentcore: append task event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agentcore: commit task event: %w", err)
	}
	return nil
}

func (s *PostgresEventStore) List(ctx context.Context, taskID string, after int64) ([]*agentv1.TaskEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM keelmesh_agent_task_events WHERE task_id = $1 AND sequence > $2 ORDER BY sequence`, taskID, after)
	if err != nil {
		return nil, fmt.Errorf("agentcore: list task events: %w", err)
	}
	defer rows.Close()
	result := make([]*agentv1.TaskEvent, 0)
	for rows.Next() {
		var payload []byte
		event := new(agentv1.TaskEvent)
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("agentcore: scan task event: %w", err)
		}
		if err := json.Unmarshal(payload, event); err != nil {
			return nil, fmt.Errorf("agentcore: decode task event: %w", err)
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agentcore: iterate task events: %w", err)
	}
	return result, nil
}

func validTaskID(id string) bool { return strings.TrimSpace(id) != "" }
