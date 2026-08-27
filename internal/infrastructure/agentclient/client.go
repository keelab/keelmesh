package agentclient

import (
	"context"
	"fmt"

	kgrpc "github.com/keelab/keelith/transport/grpc"
	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	"github.com/keelab/keelmesh/internal/application/gate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	connection *grpc.ClientConn
	managed    *kgrpc.ManagedDependency
	agent      agentv1.AgentRuntimeServiceKeelithClient
	projector  EventProjector
}

type EventProjector interface {
	ProjectTaskEvent(context.Context, *agentv1.TaskEvent) error
}

func (c *Client) SetEventProjector(projector EventProjector) { c.projector = projector }

func NewManaged(dependency *kgrpc.ManagedDependency) (*Client, error) {
	if dependency == nil {
		return nil, fmt.Errorf("agentclient: managed dependency is nil")
	}
	agent, err := agentv1.NewAgentRuntimeServiceManagedGRPCClient(dependency)
	if err != nil {
		return nil, fmt.Errorf("agentclient: bind managed dependency: %w", err)
	}
	return &Client{managed: dependency, agent: agent}, nil
}

func New(address string) (*Client, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("agentclient: dial %q: %w", address, err)
	}
	transport, err := kgrpc.NewClient(connection)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("agentclient: build governed transport: %w", err)
	}
	return &Client{connection: connection, agent: agentv1.NewAgentRuntimeServiceGRPCClient(transport)}, nil
}

func (c *Client) Close() error {
	if c != nil && c.managed != nil {
		return nil
	}
	if c == nil || c.connection == nil {
		return nil
	}
	return c.connection.Close()
}

func (c *Client) SubscribeTaskEvents(ctx context.Context, taskID string, afterSequence int64) (agentv1.AgentRuntimeService_SubscribeTaskEventsKeelithClient, error) {
	if c == nil || c.agent == nil {
		return nil, fmt.Errorf("agentclient: client is not configured")
	}
	stream, err := c.agent.SubscribeTaskEvents(ctx, &agentv1.SubscribeTaskEventsRequest{TaskId: taskID, AfterSequence: afterSequence})
	if err != nil {
		return nil, fmt.Errorf("agentclient: subscribe task events: %w", err)
	}
	return stream, nil
}

func (c *Client) Execute(ctx context.Context, task gate.Task) error {
	_, err := c.ExecuteResult(ctx, task)
	return err
}

func (c *Client) ExecuteResultWithProgress(ctx context.Context, task gate.Task, progress func(string, string)) (gate.ExecutionResult, error) {
	if c == nil || c.agent == nil {
		return gate.ExecutionResult{}, fmt.Errorf("agentclient: client is not configured")
	}
	stream, err := c.agent.SubscribeTaskEvents(ctx, &agentv1.SubscribeTaskEventsRequest{TaskId: task.ID, AfterSequence: task.EventSequence})
	if err != nil {
		return gate.ExecutionResult{}, fmt.Errorf("agentclient: subscribe task events: %w", err)
	}
	responseCh := make(chan *agentv1.ExecuteTaskResponse, 1)
	errorCh := make(chan error, 1)
	go func() {
		response, executeErr := c.agent.ExecuteTask(ctx, &agentv1.ExecuteTaskRequest{
			TaskId: task.ID, ChannelId: task.ChannelID, TargetId: task.TargetID, Content: task.Content,
			Metadata: cloneMetadata(task.Metadata),
		})
		responseCh <- response
		errorCh <- executeErr
	}()
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			response := <-responseCh
			executeErr := <-errorCh
			if executeErr != nil {
				return gate.ExecutionResult{}, fmt.Errorf("agentclient: execute task: %w", executeErr)
			}
			return executionResult(response)
		}
		if c.projector != nil {
			if err := c.projector.ProjectTaskEvent(ctx, event); err != nil {
				return gate.ExecutionResult{}, fmt.Errorf("agentclient: project task event: %w", err)
			}
		}
		if event.GetProgress() != nil && progress != nil {
			progress(event.GetProgress().GetStatus(), event.GetProgress().GetPreview())
		}
		if event.GetState() == "final" || event.GetState() == "failed" {
			response := <-responseCh
			executeErr := <-errorCh
			if executeErr != nil {
				return gate.ExecutionResult{}, fmt.Errorf("agentclient: execute task: %w", executeErr)
			}
			return executionResult(response)
		}
	}
}

func (c *Client) ExecuteResult(ctx context.Context, task gate.Task) (gate.ExecutionResult, error) {
	response, err := c.agent.ExecuteTask(ctx, &agentv1.ExecuteTaskRequest{
		TaskId: task.ID, ChannelId: task.ChannelID, TargetId: task.TargetID, Content: task.Content,
		Metadata: cloneMetadata(task.Metadata),
	})
	if err != nil {
		return gate.ExecutionResult{}, fmt.Errorf("agentclient: execute task: %w", err)
	}
	return executionResult(response)
}

func executionResult(response *agentv1.ExecuteTaskResponse) (gate.ExecutionResult, error) {
	if response == nil {
		return gate.ExecutionResult{}, fmt.Errorf("agentclient: empty execution response")
	}
	if response.GetState() == "failed" {
		return gate.ExecutionResult{}, fmt.Errorf("agentclient: task failed: %s", response.GetError())
	}
	return gate.ExecutionResult{Content: response.GetContent()}, nil
}

func cloneMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
