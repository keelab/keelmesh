package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// ReactToMessage reacts to a message.
func (s *Service) ReactToMessage(ctx context.Context, request *channelv1.ReactToMessageRequest) (*channelv1.ReactToMessageResponse, error) {
	id, err := s.runtime.ReactToMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetMessageId(), request.GetReaction())
	if err != nil {
		return nil, err
	}

	return &channelv1.ReactToMessageResponse{
		ActionId: id,
	}, nil
}
