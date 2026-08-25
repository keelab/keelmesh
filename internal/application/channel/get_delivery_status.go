package channel

import (
	"context"
	"fmt"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
)

// GetDeliveryStatus gets the delivery status of a message.
func (s *Service) GetDeliveryStatus(_ context.Context, request *channelv1.GetDeliveryStatusRequest) (*channelv1.GetDeliveryStatusResponse, error) {
	status, ok := s.runtime.Delivery(request.GetMessageId())
	if !ok {
		return nil, fmt.Errorf("channelcore: delivery %q not found", request.GetMessageId())
	}

	return &channelv1.GetDeliveryStatusResponse{
		MessageId:    status.MessageID,
		ChannelId:    status.ChannelID,
		State:        string(status.State),
		AcceptedAtMs: status.AcceptedAt.UnixMilli(),
		UpdatedAtMs:  status.UpdatedAt.UnixMilli(),
		Error:        status.Error,
	}, nil
}
