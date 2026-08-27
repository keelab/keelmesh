package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// GetChannel gets a channel.
func (s *Service) GetChannel(_ context.Context, request *channelv1.GetChannelRequest) (*channelv1.GetChannelResponse, error) {
	channel, err := s.runtime.Get(request.GetChannelId())
	if err != nil {
		if catalog, ok := s.runtime.(interface {
			Catalog() []domain.DefinitionEntity
		}); ok {
			for _, definition := range catalog.Catalog() {
				if definition.ID == request.GetChannelId() {
					return &channelv1.GetChannelResponse{Channel: channelInfo(definition, false)}, nil
				}
			}
		}
		s.logError(context.Background(), "get_channel", err, "channel_id", request.GetChannelId())
		return nil, err
	}
	definition := channel.Definition()
	return &channelv1.GetChannelResponse{
		Channel: channelInfo(definition, channel.Running()),
	}, nil
}
