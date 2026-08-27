package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// ListChannels lists all channels.
func (s *Service) ListChannels(context.Context, *channelv1.ListChannelsRequest) (*channelv1.ListChannelsResponse, error) {
	definitions := s.runtime.List()
	if catalog, ok := s.runtime.(interface {
		Catalog() []domain.DefinitionEntity
	}); ok {
		definitions = catalog.Catalog()
	}
	channels := make([]*channelv1.ChannelInfo, 0, len(definitions))
	for _, definition := range definitions {
		running := false
		if channel, err := s.runtime.Get(definition.ID); err == nil {
			running = channel.Running()
		}
		channels = append(channels, channelInfo(definition, running))
	}
	return &channelv1.ListChannelsResponse{
		Channels: channels,
	}, nil
}

func channelInfo(definition domain.DefinitionEntity, running bool) *channelv1.ChannelInfo {
	state := "disabled"
	if definition.Enabled {
		state = "stopped"
		if running {
			state = "running"
		}
	}
	healthStatus := "unknown"
	healthMessage := "channel is disabled"
	if running {
		healthStatus = "healthy"
		healthMessage = "channel is running"
	} else if definition.Enabled {
		healthStatus = "unavailable"
		healthMessage = "channel is not running"
	}
	return &channelv1.ChannelInfo{
		Id:            definition.ID,
		Kind:          definition.Kind,
		Enabled:       definition.Enabled,
		Running:       running,
		Capabilities:  append([]string(nil), definition.Capabilities...),
		State:         state,
		Configured:    definition.Enabled,
		HealthStatus:  healthStatus,
		HealthMessage: healthMessage,
	}
}
