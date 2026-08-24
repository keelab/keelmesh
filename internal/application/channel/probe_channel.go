package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

func (s *Service) ProbeChannel(ctx context.Context, request *channelv1.ProbeChannelRequest) (*channelv1.ProbeChannelResponse, error) {
	duration, err := s.runtime.Probe(ctx, request.GetChannelId())
	if err != nil {
		return nil, err
	}
	channel, _ := s.runtime.Get(request.GetChannelId())
	return &channelv1.ProbeChannelResponse{Channel: channelInfo(channel.Definition(), channel.Running()), Status: "ok", LatencyMs: duration.Milliseconds()}, nil
}
