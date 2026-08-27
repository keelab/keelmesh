package channel

import (
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// SubscribeInbound subscribes to inbound messages.
func (s *Service) SubscribeInbound(request *channelv1.SubscribeInboundRequest, stream channelv1.ChannelService_SubscribeInboundKeelithServer) error {
	queue, cancel, err := s.runtime.Subscribe(stream.Context(), request.GetChannelIds())
	if err != nil {
		s.logError(stream.Context(), "subscribe_inbound", err)
		return err
	}
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case message := <-queue:
			if err := stream.Send(&channelv1.SubscribeInboundResponse{
				Message: inboundMessage(message),
			}); err != nil {
				s.logError(stream.Context(), "subscribe_inbound.send", err)
				return err
			}
		}
	}
}

// inboundMessage converts a domain.Inbound to a channelv1.InboundMessage.
func inboundMessage(message domain.Inbound) *channelv1.InboundMessage {
	media := make([]*channelv1.MediaPart, 0, len(message.Media))
	for _, part := range message.Media {
		media = append(media, &channelv1.MediaPart{
			Type:        part.Type,
			Ref:         part.Ref,
			Caption:     part.Caption,
			Filename:    part.Filename,
			ContentType: part.ContentType,
		})
	}
	return &channelv1.InboundMessage{
		ChannelId:  message.ChannelID,
		MessageId:  message.MessageID,
		ChatId:     message.ChatID,
		SenderId:   message.SenderID,
		SenderName: message.SenderName,
		Sender: &channelv1.SenderInfo{
			Platform:    message.Sender.Platform,
			PlatformId:  message.Sender.PlatformID,
			CanonicalId: message.Sender.CanonicalID,
			Username:    message.Sender.Username,
			DisplayName: message.Sender.DisplayName,
			AvatarUrl:   message.Sender.AvatarURL,
		},
		Peer: &channelv1.Peer{
			Kind: message.Peer.Kind,
			Id:   message.Peer.ID,
		},
		Content:      message.Content,
		Metadata:     message.Metadata,
		ReceivedAtMs: message.ReceivedAt.UnixMilli(),
		Media:        media,
		SessionKey:   message.SessionKey,
		MediaScope:   message.MediaScope,
	}
}
