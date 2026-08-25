package channel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) SendMedia(ctx context.Context, message domain.OutboundMedia) (domain.ReceiptEntity, error) {
	if ctx == nil {
		return domain.ReceiptEntity{}, ErrInvalidMessage
	}
	if strings.TrimSpace(message.ChannelID) == "" || strings.TrimSpace(message.TargetID) == "" || len(message.Parts) == 0 {
		return domain.ReceiptEntity{}, ErrInvalidMessage
	}
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return domain.ReceiptEntity{}, err
	}
	if !channel.Definition().Enabled {
		return domain.ReceiptEntity{}, ErrChannelDisabled
	}
	if _, ok := channel.(domain.MediaChannel); !ok {
		return domain.ReceiptEntity{}, fmt.Errorf("channelcore: channel %q does not support media", message.ChannelID)
	}
	r.mu.RLock()
	worker := r.workers[message.ChannelID]
	r.mu.RUnlock()
	if worker == nil {
		return domain.ReceiptEntity{}, fmt.Errorf("channelcore: channel %q is not running", message.ChannelID)
	}
	if message.ID == "" {
		message.ID = fmt.Sprintf("media-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryQueued, now, "")
	select {
	case worker.media <- message:
		return domain.ReceiptEntity{MessageID: message.ID, AcceptedAt: now, State: domain.DeliveryQueued}, nil
	default:
		r.deleteDelivery(message.ID)
		return domain.ReceiptEntity{}, ErrQueueFull
	}
}
