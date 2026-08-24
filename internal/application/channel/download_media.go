package channel

import (
	"context"
	"io"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

func (s *Service) DownloadMedia(ctx context.Context, request *channelv1.DownloadMediaRequest) (*channelv1.DownloadMediaResponse, error) {
	resource, err := s.runtime.LoadMedia(ctx, request.GetRef())
	if err != nil {
		return nil, err
	}
	defer resource.Reader.Close()
	content, err := io.ReadAll(io.LimitReader(resource.Reader, 32<<20))
	if err != nil {
		return nil, err
	}
	return &channelv1.DownloadMediaResponse{Filename: resource.Filename, ContentType: resource.ContentType, Content: content}, nil
}
