package channel

import (
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

type Service struct {
	channelv1.UnimplementedChannelServiceKeelithServer
	runtime domain.ChannelDomain
}

func New(runtime domain.ChannelDomain) *Service {
	return &Service{
		runtime: runtime,
	}
}
