package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

func (s *Service) EditMessage(ctx context.Context, request *channelv1.EditMessageRequest) (*channelv1.EditMessageResponse, error) {
	err := s.runtime.EditMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetMessageId(), request.GetContent(), request.GetState(), request.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &channelv1.EditMessageResponse{MessageId: request.GetMessageId(), State: request.GetState()}, nil
}
