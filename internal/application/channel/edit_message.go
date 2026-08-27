package channel

import (
	"context"
	"maps"
	"strconv"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// EditMessage edits a message.
func (s *Service) EditMessage(ctx context.Context, request *channelv1.EditMessageRequest) (*channelv1.EditMessageResponse, error) {
	metadata := request.GetMetadata()
	if progress := request.GetProgress(); progress != nil {
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata = cloneMetadata(metadata)
		metadata["progress.status"] = progress.GetStatus()
		metadata["progress.phase"] = progress.GetPhase()
		metadata["progress.preview"] = progress.GetPreview()
		metadata["progress.model"] = progress.GetModel()
		metadata["progress.iteration"] = strconv.Itoa(int(progress.GetIteration()))
		metadata["progress.omitted"] = strconv.Itoa(int(progress.GetOmitted()))
		metadata["progress.elapsed_ms"] = strconv.FormatInt(progress.GetElapsedMs(), 10)
		metadata["progress.updated_at"] = progress.GetUpdatedAt()
	}
	err := s.runtime.EditMessage(ctx, request.GetChannelId(), request.GetTargetId(), request.GetMessageId(), request.GetContent(), request.GetState(), metadata)
	if err != nil {
		s.logError(ctx, "edit_message", err, "channel_id", request.GetChannelId(), "target_id", request.GetTargetId(), "message_id", request.GetMessageId())
		return nil, err
	}

	return &channelv1.EditMessageResponse{
		MessageId: request.GetMessageId(),
		State:     request.GetState(),
	}, nil
}

func cloneMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	maps.Copy(output, input)
	return output
}
