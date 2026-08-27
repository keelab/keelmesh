package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// SendMessage sends a message.
func (s *Service) SendMessage(ctx context.Context, request *channelv1.SendMessageRequest) (*channelv1.SendMessageResponse, error) {
	receipt, err := s.runtime.Send(ctx, domain.Outbound{
		ID:               request.GetIdempotencyKey(),
		ChannelID:        request.GetChannelId(),
		TargetID:         request.GetTargetId(),
		ReplyToMessageID: request.GetReplyToMessageId(), Content: request.GetContent(), Metadata: request.GetMetadata(),
		IdempotencyKey: request.GetIdempotencyKey(),
	})
	if err != nil {
		s.logError(ctx, "send_message", err, "channel_id", request.GetChannelId(), "target_id", request.GetTargetId())
		return nil, err
	}

	return &channelv1.SendMessageResponse{
		ChannelId:    request.GetChannelId(),
		MessageId:    receipt.MessageID,
		AcceptedAtMs: receipt.AcceptedAt.UnixMilli(),
		State:        string(receipt.State),
	}, nil
}
