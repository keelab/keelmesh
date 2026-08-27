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
	authorizer := r.outboundAuthorizer
	r.mu.RUnlock()
	if authorizer != nil {
		if err := authorizer(ctx, message); err != nil {
			return domain.ReceiptEntity{}, fmt.Errorf("channelcore: outbound authorization: %w", err)
		}
	}
	idempotencyKey := strings.TrimSpace(message.IdempotencyKey)
	if message.ID == "" {
		message.ID = fmt.Sprintf("channel-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	if idempotencyKey != "" {
		key := message.ChannelID + "\x1e" + idempotencyKey
		r.mu.Lock()
		if previous, ok := r.idempotency[key]; ok && now.Sub(previous.createdAt) <= 30*time.Minute {
			status, known := r.deliveries[previous.messageID]
			r.mu.Unlock()
			if known {
				return domain.ReceiptEntity{MessageID: previous.messageID, AcceptedAt: status.AcceptedAt, State: status.State}, nil
			}
			return domain.ReceiptEntity{
				MessageID:  previous.messageID,
				AcceptedAt: previous.createdAt,
				State:      domain.DeliveryQueued,
			}, nil
		} else {
			delete(r.idempotency, key)
			r.idempotency[key] = idempotencyEntry{messageID: message.ID, createdAt: now}
			r.mu.Unlock()
		}
	}
	r.mu.RLock()
	worker := r.workers[message.ChannelID]
	r.mu.RUnlock()
	if worker == nil {
		if idempotencyKey != "" {
			r.mu.Lock()
			delete(r.idempotency, message.ChannelID+"\x1e"+idempotencyKey)
			r.mu.Unlock()
		}
		return domain.ReceiptEntity{}, fmt.Errorf("channelcore: channel %q is not running", message.ChannelID)
	}
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryQueued, now, "")
	select {
	case worker.text <- message:
		return domain.ReceiptEntity{MessageID: message.ID, AcceptedAt: now, State: domain.DeliveryQueued}, nil
	default:
		r.deleteDelivery(message.ID)
		if idempotencyKey != "" {
			r.mu.Lock()
			delete(r.idempotency, message.ChannelID+"\x1e"+idempotencyKey)
			r.mu.Unlock()
		}
		return domain.ReceiptEntity{}, ErrQueueFull
	}
}

func (r *Repository) deleteDelivery(id string) {
	r.mu.Lock()
	delete(r.deliveries, id)
	r.mu.Unlock()
}
