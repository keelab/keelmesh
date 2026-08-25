package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// FinishStreamingMessage finishes a streaming message.
func (s *Service) FinishStreamingMessage(ctx context.Context, request *channelv1.FinishStreamingMessageRequest) (*channelv1.FinishStreamingMessageResponse, error) {
	err := s.runtime.FinishStreamingMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetMessageId(), request.GetContent())
	if err != nil {
		return nil, err
	}
	return &channelv1.FinishStreamingMessageResponse{
		Finished: true,
	}, nil
}
