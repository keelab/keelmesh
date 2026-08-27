package gate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	gatev1 "github.com/keelab/keelmesh/gen/gate/v1"
)

var (
	ErrInvalidRequest = errors.New("gatecore: invalid request")
	ErrTaskNotFound   = errors.New("gatecore: task not found")
)

// Service owns the small, transport-facing GateCore contract. Agent execution
// is deliberately a later collaborator; this service only validates, records,
// and acknowledges channel work at the Gate boundary.
type Service struct {
	gatev1.UnimplementedChannelGateServiceKeelithServer
	mu         sync.Mutex
	tasks      TaskRepository
	executor   Executor
	dispatcher OutboundDispatcher
	cancels    map[string]context.CancelFunc
	audit      AuditStore
}

// Task is the stable input passed to an optional AgentRuntime adapter.
type Task struct {
	ID            string
	ChannelID     string
	TargetID      string
	Content       string
	Metadata      map[string]string
	EventSequence int64
}

// Executor is the GateCore-to-AgentRuntime seam. GateCore keeps policy and
// task state; AgentRuntime owns model/tool execution.
type Executor interface {
	Execute(context.Context, Task) error
}

type ExecutionResult struct {
	Content string
}

type ResultExecutor interface {
	ExecuteResult(context.Context, Task) (ExecutionResult, error)
}

type ProgressExecutor interface {
	ExecuteResultWithProgress(context.Context, Task, func(string, string)) (ExecutionResult, error)
}

type OutboundDispatcher interface {
	Dispatch(context.Context, Task, ExecutionResult) error
}

type TransactionalOutboundDispatcher interface {
	OutboundDispatcher
	EnqueueFinal(context.Context, *sql.Tx, Task, ExecutionResult) error
	EnqueueProgress(context.Context, *sql.Tx, Task, string, string) error
}

type TransactionalTaskTransitioner interface {
	TransitionWithAuditAndOutbox(context.Context, string, string, TaskRecord, AuditEvent, func(context.Context, *sql.Tx) error) (bool, error)
}

type ProgressDispatcher interface {
	DispatchProgress(context.Context, Task, string, string) error
}

type TaskRecord struct {
	ChannelID       string
	TargetID        string
	Content         string
	Metadata        map[string]string
	Error           string
	Result          string
	Progress        string
	ProgressState   string
	State           string
	LastSequence    int64
	LastEventID     string
	Attempt         int
	MaxAttempts     int
	MaxWallTimeMS   int64
	RequireApproval bool
	ApprovalGranted bool
}

type TaskRepository interface {
	Get(context.Context, string) (TaskRecord, bool, error)
	Put(context.Context, string, TaskRecord) error
	ListByState(context.Context, ...string) ([]TaskEntry, error)
}

// TaskTransitioner atomically replaces a task when its current state matches
// expectedState. Durable implementations must check and write in one transaction.
type TaskTransitioner interface {
	Transition(context.Context, string, string, TaskRecord) (bool, error)
}

type AuditedTaskTransitioner interface {
	TransitionWithAudit(context.Context, string, string, TaskRecord, AuditEvent) (bool, error)
	CreateWithAudit(context.Context, string, TaskRecord, AuditEvent) error
}

type TaskEntry struct {
	ID     string
	Record TaskRecord
}

func (s *Service) transition(ctx context.Context, id, expected string, record TaskRecord) (bool, error) {
	if transitioner, ok := s.tasks.(TaskTransitioner); ok {
		return transitioner.Transition(ctx, id, expected, record)
	}
	current, exists, err := s.tasks.Get(ctx, id)
	if err != nil || !exists || current.State != expected {
		return false, err
	}
	if err := s.tasks.Put(ctx, id, record); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) recordAudit(ctx context.Context, taskID, action, fromState, toState, reason string) error {
	if s.audit == nil {
		return nil
	}
	return s.audit.Append(ctx, AuditEvent{TaskID: taskID, Action: action, FromState: fromState, ToState: toState, Reason: reason})
}

func (s *Service) transitionWithAudit(ctx context.Context, id, expected string, record TaskRecord, event AuditEvent) (bool, error) {
	if transitioner, ok := s.tasks.(AuditedTaskTransitioner); ok {
		return transitioner.TransitionWithAudit(ctx, id, expected, record, event)
	}
	applied, err := s.transition(ctx, id, expected, record)
	if err != nil || !applied {
		return applied, err
	}
	return true, s.recordAudit(ctx, event.TaskID, event.Action, event.FromState, event.ToState, event.Reason)
}

func (s *Service) transitionWithAuditAndFinalDelivery(ctx context.Context, id, expected string, record TaskRecord, event AuditEvent, task Task, result ExecutionResult, dispatcher OutboundDispatcher) (bool, error) {
	transactioner, ok := s.tasks.(TransactionalTaskTransitioner)
	transactional, hasTransactionalDispatcher := dispatcher.(TransactionalOutboundDispatcher)
	if ok && hasTransactionalDispatcher {
		return transactioner.TransitionWithAuditAndOutbox(ctx, id, expected, record, event, func(txCtx context.Context, tx *sql.Tx) error {
			return transactional.EnqueueFinal(txCtx, tx, task, result)
		})
	}
	applied, err := s.transitionWithAudit(ctx, id, expected, record, event)
	if err != nil || !applied {
		return applied, err
	}
	if dispatcher == nil {
		return true, nil
	}
	if err := dispatcher.Dispatch(ctx, task, result); err != nil {
		return false, fmt.Errorf("gatecore: dispatch final delivery: %w", err)
	}
	return true, nil
}

func (s *Service) persistProgress(ctx context.Context, task Task, state, content string, dispatcher OutboundDispatcher) error {
	record, ok, err := s.tasks.Get(ctx, task.ID)
	if err != nil {
		return fmt.Errorf("gatecore: load task progress state: %w", err)
	}
	if !ok || record.State == "final" || record.State == "failed" || record.State == "cancelled" {
		return nil
	}
	record.ProgressState = state
	record.Progress = content
	transactioner, ok := s.tasks.(TransactionalTaskTransitioner)
	transactional, hasTransactionalDispatcher := dispatcher.(TransactionalOutboundDispatcher)
	if ok && hasTransactionalDispatcher {
		_, err := transactioner.TransitionWithAuditAndOutbox(ctx, task.ID, record.State, record, AuditEvent{TaskID: task.ID, Action: "agent_progress", FromState: record.State, ToState: record.State, Reason: state}, func(txCtx context.Context, tx *sql.Tx) error {
			return transactional.EnqueueProgress(txCtx, tx, task, state, content)
		})
		return err
	}
	if err := s.tasks.Put(ctx, task.ID, record); err != nil {
		return fmt.Errorf("gatecore: persist task progress: %w", err)
	}
	if progressDispatcher, ok := dispatcher.(ProgressDispatcher); ok {
		if err := progressDispatcher.DispatchProgress(ctx, task, state, content); err != nil {
			return fmt.Errorf("gatecore: dispatch progress delivery: %w", err)
		}
	}
	return nil
}

func (s *Service) createWithAudit(ctx context.Context, id string, record TaskRecord, event AuditEvent) error {
	if transitioner, ok := s.tasks.(AuditedTaskTransitioner); ok {
		return transitioner.CreateWithAudit(ctx, id, record, event)
	}
	if err := s.tasks.Put(ctx, id, record); err != nil {
		return err
	}
	return s.recordAudit(ctx, event.TaskID, event.Action, event.FromState, event.ToState, event.Reason)
}

func (s *Service) GetTask(_ context.Context, request *gatev1.GetTaskRequest) (*gatev1.TaskStatusResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, ErrInvalidRequest
	}
	entry, ok, err := s.tasks.Get(context.Background(), request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task: %w", err)
	}
	if !ok {
		return nil, ErrTaskNotFound
	}
	return &gatev1.TaskStatusResponse{
		TaskId: request.GetTaskId(), ChannelId: entry.ChannelID, TargetId: entry.TargetID,
		State: entry.State, Error: entry.Error, ResultContent: entry.Result,
		ProgressContent: entry.Progress, ProgressState: entry.ProgressState,
	}, nil
}

func (s *Service) ProjectTaskEvent(ctx context.Context, request *gatev1.ProjectTaskEventRequest) (*gatev1.ProjectTaskEventResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" || strings.TrimSpace(request.GetEventId()) == "" || request.GetSequence() <= 0 {
		return nil, ErrInvalidRequest
	}
	record, ok, err := s.tasks.Get(ctx, request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task for event projection: %w", err)
	}
	if !ok {
		return &gatev1.ProjectTaskEventResponse{Reason: ErrTaskNotFound.Error()}, nil
	}
	if request.GetSequence() <= record.LastSequence || request.GetEventId() == record.LastEventID {
		return &gatev1.ProjectTaskEventResponse{Duplicate: true, State: record.State, Sequence: record.LastSequence}, nil
	}
	if record.State == "final" || record.State == "failed" || record.State == "cancelled" {
		return &gatev1.ProjectTaskEventResponse{Reason: "terminal task cannot be reopened", State: record.State, Sequence: record.LastSequence}, nil
	}
	expectedState := record.State
	switch request.GetState() {
	case "running", "queued":
		record.State = request.GetState()
	case "progress":
		record.ProgressState = request.GetProgressState()
		record.Progress = request.GetProgressContent()
	case "final":
		record.State = "final"
		record.Result = request.GetContent()
	case "failed":
		record.State = "failed"
		record.Error = request.GetError()
	default:
		return &gatev1.ProjectTaskEventResponse{Reason: "unsupported task event state", State: record.State, Sequence: record.LastSequence}, nil
	}
	record.LastSequence = request.GetSequence()
	record.LastEventID = request.GetEventId()
	if applied, err := s.transitionWithAudit(ctx, request.GetTaskId(), expectedState, record, AuditEvent{TaskID: request.GetTaskId(), Action: "agent_" + request.GetState(), FromState: expectedState, ToState: record.State, Reason: request.GetError()}); err != nil {
		return nil, fmt.Errorf("gatecore: persist projected task event: %w", err)
	} else if !applied {
		return &gatev1.ProjectTaskEventResponse{Reason: "task changed during event projection", State: record.State, Sequence: record.LastSequence}, nil
	}
	return &gatev1.ProjectTaskEventResponse{Applied: true, State: record.State, Sequence: record.LastSequence}, nil
}

func (s *Service) ResumeTask(_ context.Context, request *gatev1.ResumeTaskRequest) (*gatev1.DispatchResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, ErrInvalidRequest
	}
	entry, ok, err := s.tasks.Get(context.Background(), request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task: %w", err)
	}
	if !ok {
		return nil, ErrTaskNotFound
	}
	if entry.State != "failed" {
		response := &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "only failed tasks can be resumed"}
		return response, nil
	}
	entry.State = "queued"
	entry.Error = ""
	if applied, err := s.transitionWithAudit(context.Background(), request.GetTaskId(), "failed", entry, AuditEvent{TaskID: request.GetTaskId(), Action: "resumed", FromState: "failed", ToState: "queued", Reason: request.GetReason()}); err != nil {
		return nil, fmt.Errorf("gatecore: update task: %w", err)
	} else if !applied {
		return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "task changed before resume"}, nil
	}
	s.startExecution(request.GetTaskId(), entry)
	return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: "queued", Accepted: true}, nil
}

func (s *Service) ApproveTask(_ context.Context, request *gatev1.ApproveTaskRequest) (*gatev1.DispatchResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, ErrInvalidRequest
	}
	entry, ok, err := s.tasks.Get(context.Background(), request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task for approval: %w", err)
	}
	if !ok {
		return nil, ErrTaskNotFound
	}
	if !entry.RequireApproval || entry.State != "waiting_approval" {
		return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "task is not waiting for approval"}, nil
	}
	entry.ApprovalGranted = true
	entry.State = "queued"
	if applied, err := s.transitionWithAudit(context.Background(), request.GetTaskId(), "waiting_approval", entry, AuditEvent{TaskID: request.GetTaskId(), Action: "approved", FromState: "waiting_approval", ToState: "queued", Reason: request.GetReason()}); err != nil {
		return nil, fmt.Errorf("gatecore: persist task approval: %w", err)
	} else if !applied {
		return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "task changed before approval"}, nil
	}
	s.startExecution(request.GetTaskId(), entry)
	return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: "queued", Accepted: true}, nil
}

func (s *Service) CancelTask(_ context.Context, request *gatev1.CancelTaskRequest) (*gatev1.DispatchResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, ErrInvalidRequest
	}
	entry, ok, err := s.tasks.Get(context.Background(), request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task for cancellation: %w", err)
	}
	if !ok {
		return nil, ErrTaskNotFound
	}
	if entry.State == "final" || entry.State == "failed" || entry.State == "cancelled" {
		return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "task is terminal"}, nil
	}
	expectedState := entry.State
	entry.State = "cancelled"
	entry.Error = strings.TrimSpace(request.GetReason())
	if entry.Error == "" {
		entry.Error = "cancelled by operator"
	}
	if applied, err := s.transitionWithAudit(context.Background(), request.GetTaskId(), expectedState, entry, AuditEvent{TaskID: request.GetTaskId(), Action: "cancelled", FromState: expectedState, ToState: "cancelled", Reason: entry.Error}); err != nil {
		return nil, fmt.Errorf("gatecore: persist task cancellation: %w", err)
	} else if !applied {
		return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: entry.State, Accepted: false, Reason: "task changed before cancellation"}, nil
	}
	s.mu.Lock()
	if cancel := s.cancels[request.GetTaskId()]; cancel != nil {
		cancel()
		delete(s.cancels, request.GetTaskId())
	}
	s.mu.Unlock()
	return &gatev1.DispatchResponse{TaskId: request.GetTaskId(), State: "cancelled", Accepted: true}, nil
}

func New() *Service {
	return &Service{tasks: NewMemoryTaskRepository(), cancels: make(map[string]context.CancelFunc), audit: NewMemoryAuditStore()}
}

func NewWithRepository(repository TaskRepository) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("gatecore: task repository is nil")
	}
	return &Service{tasks: repository, cancels: make(map[string]context.CancelFunc), audit: NewMemoryAuditStore()}, nil
}

func (s *Service) SetAuditStore(audit AuditStore) {
	s.mu.Lock()
	s.audit = audit
	s.mu.Unlock()
}

func (s *Service) SetExecutor(executor Executor) {
	s.mu.Lock()
	s.executor = executor
	s.mu.Unlock()
}

func (s *Service) SetOutboundDispatcher(dispatcher OutboundDispatcher) {
	s.mu.Lock()
	s.dispatcher = dispatcher
	s.mu.Unlock()
}

// RecoverPending requeues durable queued/running tasks after process startup.
// Running tasks are treated as interrupted and retried from their stable task
// input; terminal tasks are never reopened.
func (s *Service) RecoverPending(ctx context.Context) error {
	entries, err := s.tasks.ListByState(ctx, "queued", "running")
	if err != nil {
		return fmt.Errorf("gatecore: list recoverable tasks: %w", err)
	}
	for _, entry := range entries {
		if entry.Record.RequireApproval && !entry.Record.ApprovalGranted {
			continue
		}
		previousState := entry.Record.State
		entry.Record.State = "queued"
		applied, transitionErr := s.transitionWithAudit(ctx, entry.ID, previousState, entry.Record, AuditEvent{TaskID: entry.ID, Action: "recovered", FromState: previousState, ToState: "queued", Reason: "process startup recovery"})
		if transitionErr != nil {
			return fmt.Errorf("gatecore: requeue task %q: %w", entry.ID, transitionErr)
		}
		if applied {
			s.startExecution(entry.ID, entry.Record)
		}
	}
	return nil
}

func (s *Service) IngestInbound(_ context.Context, request *gatev1.IngestInboundRequest) (*gatev1.DispatchResponse, error) {
	if request == nil || strings.TrimSpace(request.GetChannelId()) == "" || strings.TrimSpace(request.GetTargetId()) == "" || strings.TrimSpace(request.GetContent()) == "" {
		return nil, ErrInvalidRequest
	}
	taskID := strings.TrimSpace(request.GetRequestId())
	if taskID == "" {
		taskID = fmt.Sprintf("inbound-%s-%s", request.GetChannelId(), request.GetMessageId())
	}
	return s.accept(taskID, request.GetChannelId(), request.GetTargetId(), request.GetContent(), request.GetMetadata(), taskOptionsFromMetadata(request.GetMetadata())), nil
}

func (s *Service) AuthorizeOutbound(_ context.Context, request *gatev1.AuthorizeOutboundRequest) (*gatev1.AuthorizeOutboundResponse, error) {
	if request == nil || strings.TrimSpace(request.GetChannelId()) == "" || strings.TrimSpace(request.GetTargetId()) == "" {
		return nil, ErrInvalidRequest
	}
	response := &gatev1.AuthorizeOutboundResponse{
		RequestId: request.GetRequestId(),
		TaskId:    request.GetTaskId(),
		AgentId:   request.GetAgentId(),
		Metadata:  cloneMetadata(request.GetMetadata()),
	}
	if strings.TrimSpace(request.GetTaskId()) == "" {
		response.Reason = "task_id is required for outbound authorization"
		return response, nil
	}
	entry, ok, err := s.tasks.Get(context.Background(), request.GetTaskId())
	if err != nil {
		return nil, fmt.Errorf("gatecore: get task: %w", err)
	}
	if !ok {
		response.Reason = ErrTaskNotFound.Error()
		return response, nil
	}
	if entry.ChannelID != request.GetChannelId() || entry.TargetID != request.GetTargetId() {
		response.Reason = "outbound target does not match task"
		return response, nil
	}
	if entry.State == "cancelled" {
		response.Reason = "task is cancelled"
		return response, nil
	}
	response.Allowed = true
	return response, nil
}

func (s *Service) DispatchTask(_ context.Context, request *gatev1.DispatchTaskRequest) (*gatev1.DispatchResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" || strings.TrimSpace(request.GetChannelId()) == "" || strings.TrimSpace(request.GetTargetId()) == "" || strings.TrimSpace(request.GetContent()) == "" {
		return nil, ErrInvalidRequest
	}
	return s.accept(request.GetTaskId(), request.GetChannelId(), request.GetTargetId(), request.GetContent(), request.GetMetadata(), taskOptions{
		requireApproval: request.GetRequireApproval(),
		maxAttempts:     request.GetMaxAttempts(),
		maxWallTimeMS:   request.GetMaxWallTimeMs(),
	}), nil
}

type taskOptions struct {
	requireApproval bool
	maxAttempts     int32
	maxWallTimeMS   int64
}

func taskOptionsFromMetadata(metadata map[string]string) taskOptions {
	return taskOptions{requireApproval: metadata["require_approval"] == "true"}
}

func (s *Service) accept(taskID, channelID, targetID, content string, metadata map[string]string, options taskOptions) *gatev1.DispatchResponse {
	s.mu.Lock()
	entry, exists, err := s.tasks.Get(context.Background(), taskID)
	if err != nil {
		s.mu.Unlock()
		return &gatev1.DispatchResponse{TaskId: taskID, State: "failed", Reason: err.Error()}
	}
	if !exists {
		state := "queued"
		if options.requireApproval {
			state = "waiting_approval"
		}
		entry = TaskRecord{
			ChannelID: channelID, TargetID: targetID, Content: content,
			Metadata: cloneMetadata(metadata), State: state,
			MaxAttempts: int(options.maxAttempts), MaxWallTimeMS: options.maxWallTimeMS,
			RequireApproval: options.requireApproval,
		}
		if err := s.createWithAudit(context.Background(), taskID, entry, AuditEvent{TaskID: taskID, Action: "created", ToState: state}); err != nil {
			s.mu.Unlock()
			return &gatev1.DispatchResponse{TaskId: taskID, State: "failed", Reason: err.Error()}
		}
	}
	s.mu.Unlock()
	if !exists && entry.State == "queued" {
		s.startExecution(taskID, entry)
	}
	return &gatev1.DispatchResponse{TaskId: taskID, State: entry.State, Accepted: true}
}

func (s *Service) startExecution(taskID string, entry TaskRecord) {
	s.mu.Lock()
	executor, dispatcher := s.executor, s.dispatcher
	if executor == nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	if s.cancels == nil {
		s.cancels = make(map[string]context.CancelFunc)
	}
	s.cancels[taskID] = cancel
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.cancels, taskID)
			s.mu.Unlock()
		}()
		s.execute(ctx, taskID, entry, executor, dispatcher)
	}()
}

func (s *Service) execute(ctx context.Context, taskID string, entry TaskRecord, executor Executor, dispatcher OutboundDispatcher) {
	current, ok, lookupErr := s.tasks.Get(ctx, taskID)
	if lookupErr != nil {
		return
	}
	if !ok || current.State != "queued" {
		return
	}
	current.State = "running"
	current.Attempt++
	if current.MaxAttempts > 0 && current.Attempt > current.MaxAttempts {
		current.State = "failed"
		current.Error = "maximum attempts exceeded"
		_, _ = s.transitionWithAudit(ctx, taskID, "queued", current, AuditEvent{TaskID: taskID, Action: "failed", FromState: "queued", ToState: "failed", Reason: current.Error})
		return
	}
	started, transitionErr := s.transitionWithAudit(ctx, taskID, "queued", current, AuditEvent{TaskID: taskID, Action: "started", FromState: "queued", ToState: "running"})
	if transitionErr != nil || !started {
		return
	}
	if entry.MaxWallTimeMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(entry.MaxWallTimeMS)*time.Millisecond)
		defer cancel()
	}
	task := Task{ID: taskID, ChannelID: entry.ChannelID, TargetID: entry.TargetID, Content: entry.Content, Metadata: cloneMetadata(entry.Metadata), EventSequence: entry.LastSequence}
	result := ExecutionResult{}
	var err error
	if progressExecutor, ok := executor.(ProgressExecutor); ok {
		var progressErr error
		result, err = progressExecutor.ExecuteResultWithProgress(ctx, task, func(state, content string) {
			if progressErr == nil {
				progressErr = s.persistProgress(ctx, task, state, content, dispatcher)
			}
		})
		if err == nil {
			err = progressErr
		}
	} else if resultExecutor, ok := executor.(ResultExecutor); ok {
		result, err = resultExecutor.ExecuteResult(ctx, task)
	} else {
		err = executor.Execute(ctx, task)
	}
	persistCtx := context.WithoutCancel(ctx)
	if err != nil {
		if current, ok, getErr := s.tasks.Get(persistCtx, taskID); getErr == nil && ok {
			if current.State == "cancelled" {
				return
			}
			expectedState := current.State
			current.State = "failed"
			current.Error = err.Error()
			_, _ = s.transitionWithAudit(persistCtx, taskID, expectedState, current, AuditEvent{TaskID: taskID, Action: "failed", FromState: expectedState, ToState: "failed", Reason: current.Error})
		}
		return
	}
	if current, ok, getErr := s.tasks.Get(persistCtx, taskID); getErr == nil && ok {
		if current.State == "cancelled" {
			return
		}
		expectedState := current.State
		current.State = "final"
		current.Result = result.Content
		if _, transitionErr := s.transitionWithAuditAndFinalDelivery(persistCtx, taskID, expectedState, current, AuditEvent{TaskID: taskID, Action: "finished", FromState: expectedState, ToState: "final"}, task, result, dispatcher); transitionErr != nil {
			return
		}
	}
}

func cloneMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
