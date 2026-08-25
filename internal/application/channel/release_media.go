package channel

import (
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// ReleaseMedia releases a media.
func (s *Service) ReleaseMedia(ctx context.Context, request *channelv1.ReleaseMediaRequest) (*channelv1.ReleaseMediaResponse, error) {
	if err := s.runtime.ReleaseMedia(ctx, request.GetRef()); err != nil {
		return nil, err
	}

	return &channelv1.ReleaseMediaResponse{
		Released: true,
	}, nil
}
