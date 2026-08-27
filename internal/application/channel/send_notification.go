package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Service) SendNotification(ctx context.Context, request *channelv1.SendNotificationRequest) (*channelv1.SendNotificationResponse, error) {
	receipt, err := s.runtime.SendNotification(ctx, domain.Notification{
		ChannelID:      request.GetChannelId(),
		TargetID:       request.GetTargetId(),
		Content:        request.GetContent(),
		Metadata:       request.GetMetadata(),
		MentionIDs:     request.GetMentionIds(),
		MentionAll:     request.GetMentionAll(),
		Urgency:        request.GetUrgency(),
		IdempotencyKey: request.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, err
	}
	return &channelv1.SendNotificationResponse{
		ChannelId:         request.GetChannelId(),
		MessageId:         receipt.MessageID,
		AcceptedAtMs:      receipt.AcceptedAt.UnixMilli(),
		State:             string(receipt.State),
		InvalidMentionIds: append([]string(nil), receipt.InvalidMentionIDs...),
	}, nil
}
