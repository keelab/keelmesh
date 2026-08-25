package channel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) Send(ctx context.Context, message domain.Outbound) (domain.ReceiptEntity, error) {
	if ctx == nil {
		return domain.ReceiptEntity{}, ErrInvalidMessage
	}
	if strings.TrimSpace(message.ChannelID) == "" || strings.TrimSpace(message.TargetID) == "" || strings.TrimSpace(message.Content) == "" {
		return domain.ReceiptEntity{}, ErrInvalidMessage
	}
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return domain.ReceiptEntity{}, err
	}
	if !channel.Definition().Enabled {
		return domain.ReceiptEntity{}, ErrChannelDisabled
	}
	r.mu.RLock()
	worker := r.workers[message.ChannelID]
	r.mu.RUnlock()
	if worker == nil {
		return domain.ReceiptEntity{}, fmt.Errorf("channelcore: channel %q is not running", message.ChannelID)
	}
	if message.ID == "" {
		message.ID = fmt.Sprintf("channel-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryQueued, now, "")
	select {
	case worker.text <- message:
		return domain.ReceiptEntity{MessageID: message.ID, AcceptedAt: now, State: domain.DeliveryQueued}, nil
	default:
		r.deleteDelivery(message.ID)
		return domain.ReceiptEntity{}, ErrQueueFull
	}
}

func (r *Repository) deleteDelivery(id string) {
	r.mu.Lock()
	delete(r.deliveries, id)
	r.mu.Unlock()
}
