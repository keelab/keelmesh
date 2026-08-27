package channelclient

import (
	"context"
	"fmt"

	kgrpc "github.com/keelab/keelith/transport/grpc"
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/application/gate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	connection *grpc.ClientConn
	managed    *kgrpc.ManagedDependency
	channel    channelv1.ChannelServiceKeelithClient
}

func (c *Client) API() channelv1.ChannelServiceKeelithClient {
	if c == nil {
		return nil
	}
	return c.channel
}

func NewManaged(dependency *kgrpc.ManagedDependency) (*Client, error) {
	if dependency == nil {
		return nil, fmt.Errorf("channelclient: managed dependency is nil")
	}
	channel, err := channelv1.NewChannelServiceManagedGRPCClient(dependency)
	if err != nil {
		return nil, fmt.Errorf("channelclient: bind managed dependency: %w", err)
	}
	return &Client{managed: dependency, channel: channel}, nil
}

func New(address string) (*Client, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("channelclient: dial %q: %w", address, err)
	}
	transport, err := kgrpc.NewClient(connection)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("channelclient: build governed transport: %w", err)
	}
	return &Client{connection: connection, channel: channelv1.NewChannelServiceGRPCClient(transport)}, nil
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

func (c *Client) Dispatch(ctx context.Context, task gate.Task, result gate.ExecutionResult) error {
	if result.Content == "" {
		return fmt.Errorf("channelclient: agent result is empty")
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", task.ID, "x-idempotency-key", task.ID)
	if placeholderID := task.Metadata["placeholder_message_id"]; placeholderID != "" {
		_, err := c.channel.EditMessage(ctx, &channelv1.EditMessageRequest{ChannelId: task.ChannelID, TargetId: task.TargetID, MessageId: placeholderID, Content: result.Content, State: "final"})
		if err != nil {
			return fmt.Errorf("channelclient: finalize placeholder: %w", err)
		}
		return nil
	}
	_, err := c.channel.SendMessage(ctx, &channelv1.SendMessageRequest{
		ChannelId: task.ChannelID,
		TargetId:  task.TargetID,
		Content:   result.Content,
		Metadata: map[string]string{
			"task_id":           task.ID,
			"x-request-id":      task.ID,
			"x-idempotency-key": task.ID,
		},
		IdempotencyKey: task.ID,
	})
	if err != nil {
		return fmt.Errorf("channelclient: send task result: %w", err)
	}
	return nil
}

func (c *Client) DispatchProgress(ctx context.Context, task gate.Task, state, content string) error {
	messageID := task.Metadata["placeholder_message_id"]
	if messageID == "" || content == "" {
		return nil
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", task.ID)
	_, err := c.channel.EditMessage(ctx, &channelv1.EditMessageRequest{ChannelId: task.ChannelID, TargetId: task.TargetID, MessageId: messageID, Content: content, State: state})
	if err != nil {
		return fmt.Errorf("channelclient: update progress placeholder: %w", err)
	}
	return nil
}

// RegisterCommands publishes transport-neutral command definitions through
// ChannelCore. The channel adapter decides whether native registration is
// supported by the target platform.
func (c *Client) RegisterCommands(ctx context.Context, channelID string, commands []*channelv1.CommandDefinition) (*channelv1.RegisterCommandsResponse, error) {
	if c == nil || c.channel == nil {
		return nil, fmt.Errorf("channelclient: channel service is unavailable")
	}
	response, err := c.channel.RegisterCommands(ctx, &channelv1.RegisterCommandsRequest{
		ChannelId: channelID,
		Commands:  commands,
	})
	if err != nil {
		return nil, fmt.Errorf("channelclient: register commands: %w", err)
	}
	return response, nil
}
