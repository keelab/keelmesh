package gate

import (
	"context"
	"sync"
)

type memoryTaskRepository struct {
	mu    sync.RWMutex
	tasks map[string]TaskRecord
}

func NewMemoryTaskRepository() TaskRepository {
	return &memoryTaskRepository{tasks: make(map[string]TaskRecord)}
}

func (r *memoryTaskRepository) Get(_ context.Context, id string) (TaskRecord, bool, error) {
	r.mu.RLock()
	entry, ok := r.tasks[id]
	r.mu.RUnlock()
	return entry, ok, nil
}

func (r *memoryTaskRepository) Put(_ context.Context, id string, entry TaskRecord) error {
	r.mu.Lock()
	r.tasks[id] = entry
	r.mu.Unlock()
	return nil
}

func (r *memoryTaskRepository) Transition(_ context.Context, id, expectedState string, entry TaskRecord) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.tasks[id]
	if !ok || current.State != expectedState {
		return false, nil
	}
	r.tasks[id] = entry
	return true, nil
}

func (r *memoryTaskRepository) ListByState(_ context.Context, states ...string) ([]TaskEntry, error) {
	wanted := make(map[string]struct{}, len(states))
	for _, state := range states {
		wanted[state] = struct{}{}
	}
	r.mu.RLock()
	entries := make([]TaskEntry, 0)
	for id, record := range r.tasks {
		if _, ok := wanted[record.State]; ok {
			entries = append(entries, TaskEntry{ID: id, Record: record})
		}
	}
	r.mu.RUnlock()
	return entries, nil
}
