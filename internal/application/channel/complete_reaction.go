package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// CompleteReaction completes a reaction.
func (s *Service) CompleteReaction(_ context.Context, request *channelv1.ReactionActionRequest) (*channelv1.ReactionActionResponse, error) {
	return &channelv1.ReactionActionResponse{
		Applied: s.runtime.CompleteReaction(request.GetActionId()),
	}, nil
}
