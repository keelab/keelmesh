package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	"google.golang.org/protobuf/proto"
)

var ErrExecutionUnavailable = errors.New("agentcore: execution backend is not configured")

type Service struct {
	agentv1.UnimplementedAgentRuntimeServiceKeelithServer
	mu          sync.Mutex
	backend     Backend
	subscribers map[string]map[chan *agentv1.TaskEvent]struct{}
	lastEvents  map[string]*agentv1.TaskEvent
	events      EventStore
}

type Backend interface {
	Execute(context.Context, *agentv1.ExecuteTaskRequest) (*agentv1.ExecuteTaskResponse, error)
}

// EventBackend optionally emits sanitized progress events while executing.
// A backend remains responsible for never exposing raw model reasoning or
// credentials in the event payload.
type EventBackend interface {
	ExecuteWithEvents(context.Context, *agentv1.ExecuteTaskRequest, func(*agentv1.TaskEvent)) (*agentv1.ExecuteTaskResponse, error)
}

func New() *Service {
	return &Service{subscribers: make(map[string]map[chan *agentv1.TaskEvent]struct{}), lastEvents: make(map[string]*agentv1.TaskEvent), events: NewMemoryEventStore()}
}

func NewWithEventStore(events EventStore) (*Service, error) {
	if events == nil {
		return nil, errors.New("agentcore: event store is nil")
	}
	service := New()
	service.events = events
	return service, nil
}

func (s *Service) SetBackend(backend Backend) {
	s.backend = backend
}

func (s *Service) ExecuteTask(ctx context.Context, request *agentv1.ExecuteTaskRequest) (*agentv1.ExecuteTaskResponse, error) {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" || strings.TrimSpace(request.GetChannelId()) == "" || strings.TrimSpace(request.GetTargetId()) == "" {
		return nil, errors.New("agentcore: task_id, channel_id, and target_id are required")
	}
	s.mu.Lock()
	backend := s.backend
	s.mu.Unlock()
	if err := s.publish(ctx, &agentv1.TaskEvent{TaskId: request.GetTaskId(), State: "running"}); err != nil {
		return nil, fmt.Errorf("agentcore: publish running event: %w", err)
	}
	if backend != nil {
		if eventBackend, ok := backend.(EventBackend); ok {
			var eventErr error
			response, err := eventBackend.ExecuteWithEvents(ctx, request, func(event *agentv1.TaskEvent) {
				if publishErr := s.publish(ctx, event); publishErr != nil && eventErr == nil {
					eventErr = publishErr
				}
			})
			if eventErr != nil {
				return nil, fmt.Errorf("agentcore: publish execution event: %w", eventErr)
			}
			if publishErr := s.publish(ctx, responseEvent(request.GetTaskId(), response, err)); publishErr != nil {
				return nil, fmt.Errorf("agentcore: publish terminal event: %w", publishErr)
			}
			return response, err
		}
		response, err := backend.Execute(ctx, request)
		if publishErr := s.publish(ctx, responseEvent(request.GetTaskId(), response, err)); publishErr != nil {
			return nil, fmt.Errorf("agentcore: publish terminal event: %w", publishErr)
		}
		return response, err
	}
	response := &agentv1.ExecuteTaskResponse{
		TaskId: request.GetTaskId(),
		State:  "failed",
		Error:  ErrExecutionUnavailable.Error(),
	}
	if err := s.publish(ctx, responseEvent(request.GetTaskId(), response, nil)); err != nil {
		return nil, fmt.Errorf("agentcore: publish terminal event: %w", err)
	}
	return response, nil
}

func (s *Service) SubscribeTaskEvents(request *agentv1.SubscribeTaskEventsRequest, stream agentv1.AgentRuntimeService_SubscribeTaskEventsKeelithServer) error {
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return errors.New("agentcore: task_id is required")
	}
	ch, cancel, err := s.subscribe(stream.Context(), request.GetTaskId(), request.GetAfterSequence())
	if err != nil {
		return err
	}
	defer cancel()
	lastSequence := request.GetAfterSequence()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if event == nil {
				continue
			}
			if event.GetSequence() > 0 && event.GetSequence() <= lastSequence {
				continue
			}
			if event.GetSequence() > lastSequence {
				lastSequence = event.GetSequence()
			}
			if err := stream.Send(event); err != nil {
				return err
			}
			if event.GetState() == "final" || event.GetState() == "failed" {
				return nil
			}
		}
	}
}

func (s *Service) subscribe(ctx context.Context, taskID string, after int64) (<-chan *agentv1.TaskEvent, func(), error) {
	ch := make(chan *agentv1.TaskEvent, 16)
	s.mu.Lock()
	if s.subscribers == nil {
		s.subscribers = make(map[string]map[chan *agentv1.TaskEvent]struct{})
	}
	if s.subscribers[taskID] == nil {
		s.subscribers[taskID] = make(map[chan *agentv1.TaskEvent]struct{})
	}
	s.subscribers[taskID][ch] = struct{}{}
	if s.events != nil {
		events, err := s.events.List(ctx, taskID, after)
		if err != nil {
			s.mu.Unlock()
			delete(s.subscribers[taskID], ch)
			s.mu.Unlock()
			return nil, func() {}, fmt.Errorf("agentcore: replay task events: %w", err)
		}
		for _, event := range events {
			ch <- cloneEvent(event)
		}
	} else if event := s.lastEvents[taskID]; event != nil {
		ch <- cloneEvent(event)
	}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		if listeners := s.subscribers[taskID]; listeners != nil {
			delete(listeners, ch)
			if len(listeners) == 0 {
				delete(s.subscribers, taskID)
			}
		}
		s.mu.Unlock()
	}, nil
}

func (s *Service) publish(ctx context.Context, event *agentv1.TaskEvent) error {
	if event == nil || strings.TrimSpace(event.GetTaskId()) == "" {
		return nil
	}
	if s.events != nil {
		if err := s.events.Append(ctx, event); err != nil {
			return fmt.Errorf("append task event: %w", err)
		}
	}
	s.mu.Lock()
	if event.GetState() == "final" || event.GetState() == "failed" {
		s.lastEvents[event.GetTaskId()] = cloneEvent(event)
	}
	listeners := s.subscribers[event.GetTaskId()]
	for listener := range listeners {
		select {
		case listener <- event:
		default:
		}
	}
	s.mu.Unlock()
	return nil
}

func cloneEvent(event *agentv1.TaskEvent) *agentv1.TaskEvent {
	if event == nil {
		return nil
	}
	return proto.Clone(event).(*agentv1.TaskEvent)
}

func responseEvent(taskID string, response *agentv1.ExecuteTaskResponse, err error) *agentv1.TaskEvent {
	event := &agentv1.TaskEvent{TaskId: taskID}
	if err != nil {
		event.State = "failed"
		event.Error = err.Error()
		return event
	}
	if response == nil {
		event.State = "failed"
		event.Error = "agentcore: execution response is nil"
		return event
	}
	event.State = response.GetState()
	event.Content = response.GetContent()
	event.Error = response.GetError()
	return event
}
