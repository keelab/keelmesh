package channel

import (
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

func (s *Service) SubscribeInbound(request *channelv1.SubscribeInboundRequest, stream channelv1.ChannelService_SubscribeInboundKeelithServer) error {
	queue, cancel, err := s.runtime.Subscribe(stream.Context(), request.GetChannelIds())
	if err != nil {
		return err
	}
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case message := <-queue:
			if err := stream.Send(&channelv1.SubscribeInboundResponse{Message: inboundMessage(message)}); err != nil {
				return err
			}
		}
	}
}
func inboundMessage(message domain.Inbound) *channelv1.InboundMessage {
	media := make([]*channelv1.MediaPart, 0, len(message.Media))
	for _, part := range message.Media {
		media = append(media, &channelv1.MediaPart{Type: part.Type, Ref: part.Ref, Caption: part.Caption, Filename: part.Filename, ContentType: part.ContentType})
	}
	return &channelv1.InboundMessage{ChannelId: message.ChannelID, MessageId: message.MessageID, ChatId: message.ChatID, SenderId: message.SenderID, SenderName: message.SenderName, Content: message.Content, Metadata: message.Metadata, ReceivedAtMs: message.ReceivedAt.UnixMilli(), Media: media}
}
