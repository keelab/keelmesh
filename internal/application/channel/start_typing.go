package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// StartTyping starts typing.
func (s *Service) StartTyping(ctx context.Context, request *channelv1.StartTypingRequest) (*channelv1.StartTypingResponse, error) {
	id, err := s.runtime.StartTyping(ctx, request.GetChannelId(), request.GetTargetId())
	if err != nil {
		s.logError(ctx, "start_typing", err, "channel_id", request.GetChannelId(), "target_id", request.GetTargetId())
		return nil, err
	}

	return &channelv1.StartTypingResponse{
		ActionId: id,
	}, nil
}
