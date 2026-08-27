package gateclient

import (
	"context"
	"fmt"
	"strings"

	kgrpc "github.com/keelab/keelith/transport/grpc"
	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	gatev1 "github.com/keelab/keelmesh/gen/gate/v1"
	"github.com/keelab/keelmesh/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the optional ChannelCore client for the GateCore contract.
type Client struct {
	connection *grpc.ClientConn
	managed    *kgrpc.ManagedDependency
	transport  *kgrpc.Client
	gate       gatev1.ChannelGateServiceKeelithClient
}

func (c *Client) ProjectTaskEvent(ctx context.Context, event *agentv1.TaskEvent) error {
	if c == nil || c.gate == nil || event == nil {
		return nil
	}
	progressState, progressContent := "", ""
	if event.GetProgress() != nil {
		progressState = event.GetProgress().GetStatus()
		progressContent = event.GetProgress().GetPreview()
	}
	response, err := c.gate.ProjectTaskEvent(ctx, &gatev1.ProjectTaskEventRequest{TaskId: event.GetTaskId(), EventId: event.GetEventId(), Sequence: event.GetSequence(), State: event.GetState(), Content: event.GetContent(), Error: event.GetError(), ProgressState: progressState, ProgressContent: progressContent})
	if err != nil {
		return fmt.Errorf("gateclient: project task event: %w", err)
	}
	if response != nil && response.GetReason() != "" {
		return fmt.Errorf("gateclient: task event rejected: %s", response.GetReason())
	}
	return nil
}

func NewManaged(dependency *kgrpc.ManagedDependency) (*Client, error) {
	if dependency == nil {
		return nil, fmt.Errorf("gateclient: managed dependency is nil")
	}
	gate, err := gatev1.NewChannelGateServiceManagedGRPCClient(dependency)
	if err != nil {
		return nil, fmt.Errorf("gateclient: bind managed dependency: %w", err)
	}
	return &Client{managed: dependency, gate: gate}, nil
}

func New(address string) (*Client, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("gateclient: address is empty")
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("gateclient: dial %q: %w", address, err)
	}
	transport, err := kgrpc.NewClient(connection)
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("gateclient: build governed transport: %w", err)
	}
	return &Client{connection: connection, transport: transport, gate: gatev1.NewChannelGateServiceGRPCClient(transport)}, nil
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

func (c *Client) IngestInbound(ctx context.Context, message domain.Inbound) error {
	if c == nil || c.gate == nil {
		return nil
	}
	response, err := c.gate.IngestInbound(ctx, &gatev1.IngestInboundRequest{
		RequestId:    message.MessageID,
		ChannelId:    message.ChannelID,
		MessageId:    message.MessageID,
		TargetId:     message.ChatID,
		SenderId:     message.SenderID,
		SenderName:   message.SenderName,
		Content:      message.Content,
		Metadata:     cloneMetadata(message.Metadata),
		Media:        mediaRefs(message.Media),
		ReceivedAtMs: message.ReceivedAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("gateclient: ingest inbound: %w", err)
	}
	if response == nil {
		return fmt.Errorf("gateclient: inbound returned empty response")
	}
	if !response.GetAccepted() {
		return fmt.Errorf("gateclient: inbound rejected: %s", response.GetReason())
	}
	return nil
}

func (c *Client) AuthorizeOutbound(ctx context.Context, message domain.Outbound) error {
	if c == nil || c.gate == nil {
		return nil
	}
	if strings.TrimSpace(message.Metadata["task_id"]) == "" {
		return nil
	}
	response, err := c.gate.AuthorizeOutbound(ctx, &gatev1.AuthorizeOutboundRequest{
		RequestId: message.ID, TaskId: message.Metadata["task_id"], AgentId: message.Metadata["agent_id"],
		ChannelId: message.ChannelID, TargetId: message.TargetID,
		MessageId: message.ReplyToMessageID, Content: message.Content, Metadata: cloneMetadata(message.Metadata),
	})
	if err != nil {
		return fmt.Errorf("gateclient: authorize outbound: %w", err)
	}
	if response == nil {
		return fmt.Errorf("gateclient: outbound returned empty response")
	}
	if !response.GetAllowed() {
		return fmt.Errorf("gateclient: outbound rejected: %s", response.GetReason())
	}
	return nil
}

func (c *Client) ApproveTask(ctx context.Context, taskID, reason string) error {
	if c == nil || c.gate == nil {
		return fmt.Errorf("gateclient: gate service is unavailable")
	}
	response, err := c.gate.ApproveTask(ctx, &gatev1.ApproveTaskRequest{TaskId: taskID, Reason: reason})
	if err != nil {
		return fmt.Errorf("gateclient: approve task: %w", err)
	}
	if response == nil {
		return fmt.Errorf("gateclient: approve task returned empty response")
	}
	if !response.GetAccepted() {
		return fmt.Errorf("gateclient: approve task rejected: %s", response.GetReason())
	}
	return nil
}

func (c *Client) CancelTask(ctx context.Context, taskID, reason string) error {
	if c == nil || c.gate == nil {
		return fmt.Errorf("gateclient: gate service is unavailable")
	}
	response, err := c.gate.CancelTask(ctx, &gatev1.CancelTaskRequest{TaskId: taskID, Reason: reason})
	if err != nil {
		return fmt.Errorf("gateclient: cancel task: %w", err)
	}
	if response == nil {
		return fmt.Errorf("gateclient: cancel task returned empty response")
	}
	if !response.GetAccepted() {
		return fmt.Errorf("gateclient: cancel task rejected: %s", response.GetReason())
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

func mediaRefs(parts []domain.MediaPartEntity) []*gatev1.MediaRef {
	refs := make([]*gatev1.MediaRef, 0, len(parts))
	for _, part := range parts {
		refs = append(refs, &gatev1.MediaRef{
			Type: part.Type, Ref: part.Ref, Caption: part.Caption,
			Filename: part.Filename, ContentType: part.ContentType,
		})
	}
	return refs
}
