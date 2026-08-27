package loopcore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	loopv1 "github.com/keelab/keelmesh/gen/loop/v1"
	loopapp "github.com/keelab/keelmesh/internal/application/loop"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/agentclient"
	"github.com/keelab/keelmesh/internal/infrastructure/gateclient"
)

// bridge consumes ChannelCore's normalized inbound stream and submits each
// message to GateCore. It owns cancellation and waits for the receiver before
// allowing application shutdown to complete.
type bridge struct {
	channel    channelv1.ChannelServiceKeelithClient
	gate       *gateclient.Client
	loop       *loopapp.Service
	agent      *agentclient.Client
	cancel     context.CancelFunc
	receiverWG sync.WaitGroup
	projectWG  sync.WaitGroup
}

func (b *bridge) Name() string { return "loopcore.inbound-bridge" }

func (b *bridge) Dependencies() []string {
	return []string{"loop.channel-core", "loop.gate-core", "loop.agent-runtime"}
}

func (b *bridge) Start(ctx context.Context) error {
	if b == nil || b.channel == nil || b.gate == nil {
		return fmt.Errorf("loopcore: inbound bridge dependencies are not configured")
	}
	workerCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	stream, err := b.channel.SubscribeInbound(workerCtx, &channelv1.SubscribeInboundRequest{})
	if err != nil {
		cancel()
		return fmt.Errorf("loopcore: subscribe ChannelCore inbound: %w", err)
	}
	b.receiverWG.Add(1)
	go func() {
		defer b.receiverWG.Done()
		for {
			message, receiveErr := stream.Recv()
			if receiveErr != nil {
				return
			}
			if message == nil || message.GetMessage() == nil {
				continue
			}
			inbound := message.GetMessage()
			metadata := cloneMetadata(inbound.GetMetadata())
			prepared, prepareErr := b.channel.PrepareInbound(workerCtx, &channelv1.PrepareInboundRequest{ChannelId: inbound.GetChannelId(), TargetId: inbound.GetChatId(), MessageId: inbound.GetMessageId(), PlaceholderContent: "处理中…"})
			if prepareErr == nil && prepared != nil && prepared.GetPlaceholderMessageId() != "" {
				metadata["placeholder_message_id"] = prepared.GetPlaceholderMessageId()
			}
			inboundMessage := inboundDomain(inbound)
			inboundMessage.Metadata = metadata
			addInboundIdentityMetadata(inboundMessage, metadata)
			ingestErr := b.gate.IngestInbound(workerCtx, inboundMessage)
			if ingestErr != nil && !errors.Is(ingestErr, context.Canceled) {
				b.failInbound(workerCtx, inboundMessage, metadata["placeholder_message_id"], "请求暂时无法处理，请稍后重试。")
				continue
			}
			if ingestErr == nil && b.loop != nil {
				_, startErr := b.loop.StartRun(workerCtx, &loopv1.StartRunRequest{
					RunId: inboundMessage.MessageID, TaskId: inboundMessage.MessageID, ChannelId: inboundMessage.ChannelID,
					TargetId: inboundMessage.ChatID, Input: inboundMessage.Content, Metadata: cloneMetadata(inboundMessage.Metadata),
					Steps: []*loopv1.StepDefinition{{Id: "agent", Kind: "agent", Name: "Agent execution", MaxIterations: 1}},
				})
				if startErr != nil {
					b.failInbound(workerCtx, inboundMessage, metadata["placeholder_message_id"], "请求启动失败，请稍后重试。")
					continue
				}
				if b.agent != nil && b.loop != nil {
					b.projectWG.Add(1)
					go func(taskID string) {
						defer b.projectWG.Done()
						b.projectAgentEvents(workerCtx, taskID)
					}(inboundMessage.MessageID)
				}
			}
		}
	}()
	return nil
}

func (b *bridge) failInbound(ctx context.Context, message domain.Inbound, placeholderID, content string) {
	if b == nil || b.channel == nil || placeholderID == "" {
		return
	}
	_, _ = b.channel.EditMessage(ctx, &channelv1.EditMessageRequest{
		ChannelId: message.ChannelID,
		TargetId:  message.ChatID,
		MessageId: placeholderID,
		Content:   content,
		State:     "failed",
	})
}

func addInboundIdentityMetadata(message domain.Inbound, metadata map[string]string) {
	if message.Sender.CanonicalID != "" {
		metadata["sender.canonical_id"] = message.Sender.CanonicalID
	}
	if message.Sender.Platform != "" {
		metadata["sender.platform"] = message.Sender.Platform
	}
	if message.Sender.PlatformID != "" {
		metadata["sender.platform_id"] = message.Sender.PlatformID
	}
	if message.Peer.Kind != "" {
		metadata["peer.kind"] = message.Peer.Kind
	}
	if message.Peer.ID != "" {
		metadata["peer.id"] = message.Peer.ID
	}
	if message.SessionKey != "" {
		metadata["session_key"] = message.SessionKey
	}
	if message.MediaScope != "" {
		metadata["media_scope"] = message.MediaScope
	}
}

func (b *bridge) projectAgentEvents(ctx context.Context, taskID string) {
	stream, err := b.agent.SubscribeTaskEvents(ctx, taskID, 0)
	if err != nil {
		return
	}
	for {
		event, err := stream.Recv()
		if err != nil || event == nil {
			return
		}
		state, output := event.GetState(), event.GetContent()
		if progress := event.GetProgress(); progress != nil && state != "final" && state != "failed" {
			state = "running"
			output = progress.GetPreview()
		}
		if state == "running" && event.GetProgress() == nil {
			continue
		}
		iteration := int32(0)
		if progress := event.GetProgress(); progress != nil {
			iteration = progress.GetIteration()
		}
		_, _ = b.loop.AdvanceStep(ctx, &loopv1.AdvanceStepRequest{RunId: taskID, StepId: "agent", State: mapAgentStepState(state), Output: output, Error: event.GetError(), Iteration: iteration})
		if state == "final" || state == "failed" {
			return
		}
	}
}

func mapAgentStepState(state string) string {
	if state == "final" || state == "failed" {
		return state
	}
	return "running"
}

func cloneMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func (b *bridge) Stop(context.Context) error {
	if b == nil {
		return nil
	}
	if b.cancel != nil {
		b.cancel()
	}
	b.receiverWG.Wait()
	b.projectWG.Wait()
	return nil
}

func inboundDomain(message *channelv1.InboundMessage) domain.Inbound {
	media := make([]domain.MediaPartEntity, 0, len(message.GetMedia()))
	for _, part := range message.GetMedia() {
		media = append(media, domain.MediaPartEntity{Type: part.GetType(), Ref: part.GetRef(), Caption: part.GetCaption(), Filename: part.GetFilename(), ContentType: part.GetContentType()})
	}
	receivedAt := time.Now()
	if message.GetReceivedAtMs() > 0 {
		receivedAt = time.UnixMilli(message.GetReceivedAtMs())
	}
	sender := message.GetSender()
	peer := message.GetPeer()
	inbound := domain.Inbound{
		ChannelID:  message.GetChannelId(),
		MessageID:  message.GetMessageId(),
		ChatID:     message.GetChatId(),
		SenderID:   message.GetSenderId(),
		SenderName: message.GetSenderName(),
		Content:    message.GetContent(),
		Metadata:   message.GetMetadata(),
		Media:      media,
		SessionKey: message.GetSessionKey(),
		MediaScope: message.GetMediaScope(),
		ReceivedAt: receivedAt,
	}
	if sender != nil {
		inbound.Sender = domain.SenderInfo{
			Platform:    sender.GetPlatform(),
			PlatformID:  sender.GetPlatformId(),
			CanonicalID: sender.GetCanonicalId(),
			Username:    sender.GetUsername(),
			DisplayName: sender.GetDisplayName(),
			AvatarURL:   sender.GetAvatarUrl(),
		}
	}
	if peer != nil {
		inbound.Peer = domain.Peer{Kind: peer.GetKind(), ID: peer.GetId()}
	}
	return inbound
}
