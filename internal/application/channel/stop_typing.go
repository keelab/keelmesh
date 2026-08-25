package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// StopTyping stops typing.
func (s *Service) StopTyping(_ context.Context, request *channelv1.StopTypingRequest) (*channelv1.StopTypingResponse, error) {
	return &channelv1.StopTypingResponse{
		Stopped: s.runtime.StopTyping(request.GetActionId()),
	}, nil
}
