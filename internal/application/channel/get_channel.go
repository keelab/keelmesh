package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

func (s *Service) GetChannel(_ context.Context, request *channelv1.GetChannelRequest) (*channelv1.GetChannelResponse, error) {
	channel, err := s.runtime.Get(request.GetChannelId())
	if err != nil {
		return nil, err
	}
	definition := channel.Definition()
	return &channelv1.GetChannelResponse{Channel: channelInfo(definition, channel.Running())}, nil
}
