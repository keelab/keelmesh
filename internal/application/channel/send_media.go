package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Service) SendMedia(ctx context.Context, request *channelv1.SendMediaRequest) (*channelv1.SendMediaResponse, error) {
	parts := make([]domain.MediaPartEntity, 0, len(request.GetParts()))
	for _, part := range request.GetParts() {
		parts = append(parts, domain.MediaPartEntity{Type: part.GetType(), Ref: part.GetRef(), Caption: part.GetCaption(), Filename: part.GetFilename(), ContentType: part.GetContentType()})
	}
	receipt, err := s.runtime.SendMedia(ctx, domain.OutboundMedia{ID: request.GetIdempotencyKey(), ChannelID: request.GetChannelId(), TargetID: request.GetTargetId(), Parts: parts, Metadata: request.GetMetadata(), IdempotencyKey: request.GetIdempotencyKey()})
	if err != nil {
		return nil, err
	}
	return &channelv1.SendMediaResponse{ChannelId: request.GetChannelId(), MessageId: receipt.MessageID, AcceptedAtMs: receipt.AcceptedAt.UnixMilli(), State: string(receipt.State)}, nil
}
