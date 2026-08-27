package agent

import (
	"context"
	"fmt"
	"strings"

	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
)

// EchoBackend is a deterministic local backend for wiring and transport
// validation. It is intentionally opt-in and must not be treated as an Agent
// or model implementation.
type EchoBackend struct{}

func (EchoBackend) Execute(ctx context.Context, request *agentv1.ExecuteTaskRequest) (*agentv1.ExecuteTaskResponse, error) {
	return EchoBackend{}.execute(ctx, request, nil)
}

func (EchoBackend) ExecuteWithEvents(ctx context.Context, request *agentv1.ExecuteTaskRequest, publish func(*agentv1.TaskEvent)) (*agentv1.ExecuteTaskResponse, error) {
	if publish != nil {
		publish(&agentv1.TaskEvent{TaskId: request.GetTaskId(), State: "progress", Progress: &agentv1.ProgressSnapshot{Status: "running", Phase: "echo", Preview: "processing"}})
	}
	return EchoBackend{}.execute(ctx, request, publish)
}

func (EchoBackend) execute(ctx context.Context, request *agentv1.ExecuteTaskRequest, _ func(*agentv1.TaskEvent)) (*agentv1.ExecuteTaskResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("echo backend: context canceled: %w", err)
	}
	content := strings.TrimSpace(request.GetContent())
	if content == "" {
		return nil, fmt.Errorf("echo backend: content is empty")
	}
	return &agentv1.ExecuteTaskResponse{TaskId: request.GetTaskId(), State: "final", Content: "[echo] " + content}, nil
}
