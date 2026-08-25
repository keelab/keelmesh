package channel

import (
	"bytes"
	"context"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// UploadMedia uploads a media file.
func (s *Service) UploadMedia(ctx context.Context, request *channelv1.UploadMediaRequest) (*channelv1.UploadMediaResponse, error) {
	part, err := s.runtime.StoreMedia(ctx, request.GetFilename(), request.GetContentType(), bytes.NewReader(request.GetContent()))
	if err != nil {
		return nil, err
	}

	return &channelv1.UploadMediaResponse{
		Media: &channelv1.MediaPart{
			Type:        "file",
			Ref:         part.Ref,
			Filename:    part.Filename,
			ContentType: part.ContentType},
	}, nil
}
