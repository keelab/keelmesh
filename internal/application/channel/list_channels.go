package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Service) ListChannels(context.Context, *channelv1.ListChannelsRequest) (*channelv1.ListChannelsResponse, error) {
	definitions := s.runtime.List()
	channels := make([]*channelv1.ChannelInfo, 0, len(definitions))
	for _, definition := range definitions {
		channel, _ := s.runtime.Get(definition.ID)
		channels = append(channels, channelInfo(definition, channel.Running()))
	}
	return &channelv1.ListChannelsResponse{Channels: channels}, nil
}

func channelInfo(definition domain.DefinitionEntity, running bool) *channelv1.ChannelInfo {
	return &channelv1.ChannelInfo{Id: definition.ID, Kind: definition.Kind, Enabled: definition.Enabled, Running: running, Capabilities: append([]string(nil), definition.Capabilities...)}
}
