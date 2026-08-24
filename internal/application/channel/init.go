package channel

import (
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Service struct {
	channelv1.UnimplementedChannelServiceKeelithServer
	runtime *channelruntime.Runtime
}

func New(runtime *channelruntime.Runtime) *Service {
	return &Service{
		runtime: runtime,
	}
}
