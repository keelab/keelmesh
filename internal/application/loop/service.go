package loop

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	loopv1 "github.com/keelab/keelmesh/gen/loop/v1"
)

var (
	ErrInvalidRequest = errors.New("loopcore: invalid request")
	ErrRunNotFound    = errors.New("loopcore: run not found")
)

type Step struct {
	ID            string
	Kind          string
	Name          string
	State         string
	Iteration     int32
	MaxIterations int32
	Output        string
	Error         string
}

type Run struct {
	TaskID        string
	ChannelID     string
	TargetID      string
	Input         string
	Metadata      map[string]string
	State         string
	CurrentStepID string
	Iteration     int32
	Output        string
	Error         string
	Steps         []Step
}

type Service struct {
	loopv1.UnimplementedLoopServiceKeelithServer
	mu    sync.Mutex
	store RunRepository
}

type RunRepository interface {
	Get(context.Context, string) (Run, bool, error)
	Put(context.Context, string, Run) error
}

type memoryRepository struct {
	mu   sync.RWMutex
	runs map[string]Run
}

func New() *Service { return &Service{store: &memoryRepository{runs: make(map[string]Run)}} }

func NewWithRepository(store RunRepository) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("loopcore: run repository is nil")
	}
	return &Service{store: store}, nil
}

func (r *memoryRepository) Get(_ context.Context, id string) (Run, bool, error) {
	r.mu.RLock()
	run, ok := r.runs[id]
	r.mu.RUnlock()
	return cloneRun(run), ok, nil
}

func (r *memoryRepository) Put(_ context.Context, id string, run Run) error {
	r.mu.Lock()
	r.runs[id] = cloneRun(run)
	r.mu.Unlock()
	return nil
}

func (s *Service) StartRun(ctx context.Context, request *loopv1.StartRunRequest) (*loopv1.RunResponse, error) {
	if request == nil || strings.TrimSpace(request.GetRunId()) == "" || strings.TrimSpace(request.GetTaskId()) == "" || strings.TrimSpace(request.GetInput()) == "" {
		return nil, ErrInvalidRequest
	}
	steps := make([]Step, 0, len(request.GetSteps()))
	for _, definition := range request.GetSteps() {
		if definition == nil || strings.TrimSpace(definition.GetId()) == "" {
			return nil, fmt.Errorf("%w: step id is required", ErrInvalidRequest)
		}
		steps = append(steps, Step{ID: definition.GetId(), Kind: definition.GetKind(), Name: definition.GetName(), State: "queued", MaxIterations: definition.GetMaxIterations()})
	}
	state := "final"
	current := ""
	if len(steps) > 0 {
		state = "running"
		current = steps[0].ID
		steps[0].State = "running"
	}
	run := Run{TaskID: request.GetTaskId(), ChannelID: request.GetChannelId(), TargetID: request.GetTargetId(), Input: request.GetInput(), Metadata: cloneMetadata(request.GetMetadata()), State: state, CurrentStepID: current, Steps: steps}
	s.mu.Lock()
	_, exists, err := s.store.Get(ctx, request.GetRunId())
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("loopcore: check run: %w", err)
	}
	if exists {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: run already exists", ErrInvalidRequest)
	}
	if err := s.store.Put(ctx, request.GetRunId(), run); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("loopcore: persist run: %w", err)
	}
	s.mu.Unlock()
	return response(request.GetRunId(), run), nil
}

func (s *Service) GetRun(ctx context.Context, request *loopv1.GetRunRequest) (*loopv1.RunResponse, error) {
	if request == nil || strings.TrimSpace(request.GetRunId()) == "" {
		return nil, ErrInvalidRequest
	}
	run, ok, err := s.store.Get(ctx, request.GetRunId())
	if err != nil {
		return nil, fmt.Errorf("loopcore: get run: %w", err)
	}
	if !ok {
		return nil, ErrRunNotFound
	}
	return response(request.GetRunId(), run), nil
}

func (s *Service) AdvanceStep(ctx context.Context, request *loopv1.AdvanceStepRequest) (*loopv1.RunResponse, error) {
	if request == nil || strings.TrimSpace(request.GetRunId()) == "" || strings.TrimSpace(request.GetStepId()) == "" || request.GetIteration() < 0 {
		return nil, ErrInvalidRequest
	}
	s.mu.Lock()
	run, ok, err := s.store.Get(ctx, request.GetRunId())
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("loopcore: load run: %w", err)
	}
	if !ok {
		s.mu.Unlock()
		return nil, ErrRunNotFound
	}
	if run.State == "final" || run.State == "failed" || run.State == "cancelled" {
		s.mu.Unlock()
		return response(request.GetRunId(), run), nil
	}
	index := -1
	for i := range run.Steps {
		if run.Steps[i].ID == request.GetStepId() {
			index = i
			break
		}
	}
	if index < 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: step %q", ErrInvalidRequest, request.GetStepId())
	}
	step := &run.Steps[index]
	if request.GetState() != "running" && request.GetState() != "final" && request.GetState() != "failed" {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: unsupported step state %q", ErrInvalidRequest, request.GetState())
	}
	if step.MaxIterations > 0 && request.GetIteration() > step.MaxIterations {
		step.State = "failed"
		step.Error = "maximum iterations exceeded"
		run.State = "failed"
		run.Error = step.Error
		run.Iteration = request.GetIteration()
		if err := s.store.Put(ctx, request.GetRunId(), run); err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("loopcore: persist iteration failure: %w", err)
		}
		s.mu.Unlock()
		return response(request.GetRunId(), run), nil
	}
	step.State = request.GetState()
	step.Output = request.GetOutput()
	step.Error = request.GetError()
	step.Iteration = request.GetIteration()
	if step.State == "failed" {
		run.State, run.Error = "failed", step.Error
	} else if step.State == "final" {
		run.Output = step.Output
		if index+1 < len(run.Steps) {
			run.CurrentStepID = run.Steps[index+1].ID
			run.Steps[index+1].State = "running"
		} else {
			run.State, run.CurrentStepID = "final", ""
		}
	}
	run.Iteration = request.GetIteration()
	if err := s.store.Put(ctx, request.GetRunId(), run); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("loopcore: persist step: %w", err)
	}
	s.mu.Unlock()
	return response(request.GetRunId(), run), nil
}

func (s *Service) CancelRun(ctx context.Context, request *loopv1.CancelRunRequest) (*loopv1.RunResponse, error) {
	if request == nil || strings.TrimSpace(request.GetRunId()) == "" {
		return nil, ErrInvalidRequest
	}
	s.mu.Lock()
	run, ok, err := s.store.Get(ctx, request.GetRunId())
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("loopcore: load run for cancellation: %w", err)
	}
	if !ok {
		s.mu.Unlock()
		return nil, ErrRunNotFound
	}
	if run.State != "final" && run.State != "failed" && run.State != "cancelled" {
		run.State = "cancelled"
		run.Error = strings.TrimSpace(request.GetReason())
		if run.Error == "" {
			run.Error = "cancelled by operator"
		}
		if err := s.store.Put(ctx, request.GetRunId(), run); err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("loopcore: persist cancellation: %w", err)
		}
	}
	s.mu.Unlock()
	return response(request.GetRunId(), run), nil
}

func cloneRun(run Run) Run {
	run.Metadata = cloneMetadata(run.Metadata)
	run.Steps = append([]Step(nil), run.Steps...)
	return run
}

type PostgresRepository struct{ db *sql.DB }

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("loopcore: run database is nil")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) EnsureSchema(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS keelmesh_loop_runs (
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
	)`)
	if err != nil {
		return fmt.Errorf("loopcore: ensure run schema: %w", err)
	}
	return nil
}

func (r *PostgresRepository) Get(ctx context.Context, id string) (Run, bool, error) {
	var run Run
	var metadata, steps []byte
	err := r.db.QueryRowContext(ctx, `SELECT task_id, channel_id, target_id, input, metadata, state, current_step_id, iteration, output, error, steps FROM keelmesh_loop_runs WHERE run_id=$1`, id).Scan(&run.TaskID, &run.ChannelID, &run.TargetID, &run.Input, &metadata, &run.State, &run.CurrentStepID, &run.Iteration, &run.Output, &run.Error, &steps)
	if err == sql.ErrNoRows {
		return Run{}, false, nil
	}
	if err != nil {
		return Run{}, false, fmt.Errorf("loopcore: get run %q: %w", id, err)
	}
	if err := json.Unmarshal(metadata, &run.Metadata); err != nil {
		return Run{}, false, fmt.Errorf("loopcore: decode run metadata: %w", err)
	}
	if err := json.Unmarshal(steps, &run.Steps); err != nil {
		return Run{}, false, fmt.Errorf("loopcore: decode run steps: %w", err)
	}
	return run, true, nil
}

func (r *PostgresRepository) Put(ctx context.Context, id string, run Run) error {
	metadata, err := json.Marshal(run.Metadata)
	if err != nil {
		return fmt.Errorf("loopcore: encode run metadata: %w", err)
	}
	steps, err := json.Marshal(run.Steps)
	if err != nil {
		return fmt.Errorf("loopcore: encode run steps: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO keelmesh_loop_runs(run_id,task_id,channel_id,target_id,input,metadata,state,current_step_id,iteration,output,error,steps) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12::jsonb) ON CONFLICT(run_id) DO UPDATE SET task_id=EXCLUDED.task_id,channel_id=EXCLUDED.channel_id,target_id=EXCLUDED.target_id,input=EXCLUDED.input,metadata=EXCLUDED.metadata,state=EXCLUDED.state,current_step_id=EXCLUDED.current_step_id,iteration=EXCLUDED.iteration,output=EXCLUDED.output,error=EXCLUDED.error,steps=EXCLUDED.steps,updated_at=now()`, id, run.TaskID, run.ChannelID, run.TargetID, run.Input, string(metadata), run.State, run.CurrentStepID, run.Iteration, run.Output, run.Error, string(steps))
	if err != nil {
		return fmt.Errorf("loopcore: persist run %q: %w", id, err)
	}
	return nil
}

func cloneMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func response(id string, run Run) *loopv1.RunResponse {
	steps := make([]*loopv1.StepStatus, 0, len(run.Steps))
	for _, step := range run.Steps {
		steps = append(steps, &loopv1.StepStatus{Id: step.ID, Kind: step.Kind, Name: step.Name, State: step.State, Iteration: step.Iteration, Output: step.Output, Error: step.Error})
	}
	return &loopv1.RunResponse{RunId: id, TaskId: run.TaskID, ChannelId: run.ChannelID, TargetId: run.TargetID, State: run.State, CurrentStepId: run.CurrentStepID, Iteration: run.Iteration, Output: run.Output, Error: run.Error, Steps: steps}
}
