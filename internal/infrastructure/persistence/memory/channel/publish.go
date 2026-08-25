package channel

import (
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/platform/clock"
)

func (r *Repository) Publish(message domain.Inbound) {
	if message.ReceivedAt.IsZero() {
		message.ReceivedAt = clock.UTC()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, sub := range r.subs {
		if len(sub.channels) > 0 {
			if _, ok := sub.channels[message.ChannelID]; !ok {
				continue
			}
		}
		select {
		case sub.queue <- message:
		default:
		}
	}
}
