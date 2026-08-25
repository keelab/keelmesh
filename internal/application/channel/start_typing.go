package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// StartTyping starts typing.
func (s *Service) StartTyping(ctx context.Context, request *channelv1.StartTypingRequest) (*channelv1.StartTypingResponse, error) {
	id, err := s.runtime.StartTyping(ctx, request.GetChannelId(), request.GetTargetId())
	if err != nil {
		return nil, err
	}

	return &channelv1.StartTypingResponse{
		ActionId: id,
	}, nil
}
