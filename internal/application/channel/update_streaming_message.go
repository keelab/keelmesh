package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// UpdateStreamingMessage updates a streaming message.
func (s *Service) UpdateStreamingMessage(ctx context.Context, request *channelv1.UpdateStreamingMessageRequest) (*channelv1.UpdateStreamingMessageResponse, error) {
	err := s.runtime.UpdateStreamingMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetMessageId(), request.GetContent())
	if err != nil {
		return nil, err
	}

	return &channelv1.UpdateStreamingMessageResponse{
		Updated: true,
	}, nil
}
