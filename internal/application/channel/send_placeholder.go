package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// SendPlaceholder sends a placeholder.
func (s *Service) SendPlaceholder(ctx context.Context, request *channelv1.SendPlaceholderRequest) (*channelv1.SendPlaceholderResponse, error) {
	id, err := s.runtime.SendPlaceholder(ctx, request.GetChannelId(), request.GetTargetId(), request.GetReplyToMessageId(), request.GetContent())
	if err != nil {
		return nil, err
	}

	return &channelv1.SendPlaceholderResponse{
		MessageId: id,
	}, nil
}
