package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

func (s *Service) ExpireReaction(_ context.Context, request *channelv1.ReactionActionRequest) (*channelv1.ReactionActionResponse, error) {
	return &channelv1.ReactionActionResponse{Applied: s.runtime.ExpireReaction(request.GetActionId())}, nil
}
