package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Service) PrepareInbound(ctx context.Context, request *channelv1.PrepareInboundRequest) (*channelv1.PrepareInboundResponse, error) {
	receipt, err := s.runtime.PrepareInbound(ctx, domain.InboundPreparation{
		ChannelID:          request.GetChannelId(),
		TargetID:           request.GetTargetId(),
		MessageID:          request.GetMessageId(),
		ReplyToMessageID:   request.GetReplyToMessageId(),
		PlaceholderContent: request.GetPlaceholderContent(),
		Reaction:           request.GetReaction(),
	})
	if err != nil {
		return nil, err
	}
	return &channelv1.PrepareInboundResponse{
		TypingActionId:       receipt.TypingActionID,
		ReactionActionId:     receipt.ReactionActionID,
		PlaceholderMessageId: receipt.PlaceholderMessageID,
	}, nil
}
