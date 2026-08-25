package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// StartStreamingMessage starts a streaming message.
func (s *Service) StartStreamingMessage(ctx context.Context, request *channelv1.StartStreamingMessageRequest) (*channelv1.StartStreamingMessageResponse, error) {
	id, err := s.runtime.StartStreamingMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetReplyToMessageId(), request.GetContent())
	if err != nil {
		return nil, err
	}

	return &channelv1.StartStreamingMessageResponse{
		MessageId: id,
	}, nil
}
