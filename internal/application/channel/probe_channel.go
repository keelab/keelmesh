package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// ProbeChannel probes a channel.
func (s *Service) ProbeChannel(ctx context.Context, request *channelv1.ProbeChannelRequest) (*channelv1.ProbeChannelResponse, error) {
	duration, err := s.runtime.Probe(ctx, request.GetChannelId())
	if err != nil {
		if catalog, ok := s.runtime.(interface {
			Catalog() []domain.DefinitionEntity
		}); ok {
			for _, definition := range catalog.Catalog() {
				if definition.ID == request.GetChannelId() {
					info := channelInfo(definition, false)
					info.HealthStatus = "unknown"
					info.HealthMessage = "channel is not initialized"
					return &channelv1.ProbeChannelResponse{Channel: info, Status: "unknown"}, nil
				}
			}
		}
		s.logError(ctx, "probe_channel", err, "channel_id", request.GetChannelId())
		return nil, err
	}
	channel, err := s.runtime.Get(request.GetChannelId())
	if err != nil {
		return nil, err
	}
	info := channelInfo(channel.Definition(), channel.Running())
	info.HealthStatus = "healthy"
	info.HealthMessage = "channel probe passed"

	return &channelv1.ProbeChannelResponse{
		Channel:   info,
		Status:    "ok",
		LatencyMs: duration.Milliseconds(),
	}, nil
}
