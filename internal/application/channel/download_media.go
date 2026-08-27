package channel

import (
	"context"
	"io"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// DownloadMedia downloads media.
func (s *Service) DownloadMedia(ctx context.Context, request *channelv1.DownloadMediaRequest) (*channelv1.DownloadMediaResponse, error) {
	resource, err := s.runtime.LoadMedia(ctx, request.GetRef())
	if err != nil {
		s.logError(ctx, "load_media", err, "ref", request.GetRef())
		return nil, err
	}
	defer resource.Reader.Close()

	content, err := io.ReadAll(io.LimitReader(resource.Reader, 32<<20)) // 32MB
	if err != nil {
		s.logError(ctx, "read_media", err, "ref", request.GetRef())
		return nil, err
	}

	return &channelv1.DownloadMediaResponse{
		Filename:    resource.Filename,
		ContentType: resource.ContentType,
		Content:     content,
	}, nil
}
