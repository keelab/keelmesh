package channel

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidMessage
	}
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return errors.New("channelcore: runtime is already running")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.janitorDone = make(chan struct{})
	r.mu.Unlock()
	go r.runJanitor(runCtx)
	started := make([]domain.Channel, 0)
	for _, channel := range r.registry.All() {
		if !channel.Definition().Enabled {
			continue
		}
		if err := channel.Start(runCtx, r.Publish); err != nil {
			cancel()
			for _, s := range slices.Backward(started) {
				_ = s.Stop(context.WithoutCancel(ctx))
			}
			return fmt.Errorf("start channel %q: %w", channel.Definition().ID, err)
		}
		started = append(started, channel)
		definition := channel.Definition()
		queueSize := definition.QueueSize
		if queueSize <= 0 {
			queueSize = 16
		}
		worker := &channelWorker{
			channel: channel,
			text:    make(chan domain.Outbound, queueSize),
			media:   make(chan domain.OutboundMedia, queueSize),
			stop:    make(chan struct{}),
			limiter: newRateLimiter(
				definition.RatePerSecond,
				definition.Burst,
			)}
		r.mu.Lock()
		r.workers[channel.Definition().ID] = worker
		r.mu.Unlock()
		worker.wg.Add(1)
		go r.runWorker(runCtx, worker)
	}
	return nil
}

func (r *Repository) runJanitor(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	defer func() {
		r.mu.RLock()
		done := r.janitorDone
		r.mu.RUnlock()
		if done != nil {
			close(done)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.mu.Lock()
			stops := make([]func(), 0)
			expires := make([]func(), 0)
			for id, entry := range r.actions {
				if now.Sub(entry.createdAt) > 5*time.Minute {
					delete(r.actions, id)
					if entry.stop != nil {
						stops = append(stops, entry.stop)
					}
				}
			}
			for id, entry := range r.reactions {
				if now.Sub(entry.createdAt) > 30*time.Minute {
					delete(r.reactions, id)
					if entry.expire != nil {
						expires = append(expires, entry.expire)
					}
				}
			}
			r.mu.Unlock()
			for _, stop := range stops {
				stop()
			}
			for _, expire := range expires {
				expire()
			}
		}
	}
}

func (r *Repository) runWorker(ctx context.Context, worker *channelWorker) {
	defer worker.wg.Done()
	for {
		select {
		case message := <-worker.text:
			r.sendWithRetry(ctx, worker, message)
		case message := <-worker.media:
			if channel, ok := worker.channel.(domain.MediaChannel); ok {
				r.sendMediaWithRetry(ctx, worker, channel, message)
			}
		case <-worker.stop:
			r.drainWorker(ctx, worker)
			return
		case <-ctx.Done():
			r.drainWorker(ctx, worker)
			return
		}
	}
}
func (r *Repository) sendWithRetry(ctx context.Context, worker *channelWorker, message domain.Outbound) {
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliverySending, time.Now().UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
		if receipt, err := worker.channel.Send(ctx, message); err == nil {
			id := message.ID
			if receipt.MessageID != "" {
				id = receipt.MessageID
			}
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryAcknowledged, time.Now().UTC(), "")
			_ = id
			return
		} else if attempt == maxRetries-1 {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryFailed, time.Now().UTC(), err.Error())
			return
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
	}
}
func (r *Repository) sendMediaWithRetry(ctx context.Context, worker *channelWorker, channel domain.MediaChannel, message domain.OutboundMedia) {
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliverySending, time.Now().UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
		if _, err := channel.SendMedia(ctx, message); err == nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryAcknowledged, time.Now().UTC(), "")
			return
		} else if attempt == maxRetries-1 {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryFailed, time.Now().UTC(), err.Error())
			return
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
	}
}

func (l *rateLimiter) wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens = minFloat(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
		l.last = now
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
}
func (r *Repository) recordDelivery(id, channelID string, state domain.DeliveryState, updated time.Time, messageErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := r.deliveries[id]
	status.MessageID, status.ChannelID, status.State, status.UpdatedAt = id, channelID, state, updated
	if status.AcceptedAt.IsZero() {
		status.AcceptedAt = updated
	}
	status.Error = messageErr
	r.deliveries[id] = status
}

func (r *Repository) drainWorker(ctx context.Context, worker *channelWorker) {
	for {
		select {
		case message := <-worker.text:
			r.sendWithRetry(ctx, worker, message)
		case message := <-worker.media:
			if channel, ok := worker.channel.(domain.MediaChannel); ok {
				r.sendMediaWithRetry(ctx, worker, channel, message)
			}
		default:
			return
		}
	}
}

func newRateLimiter(rate float64, burst int) *rateLimiter {
	if rate <= 0 {
		rate = 10
	}
	if burst <= 0 {
		burst = 16
	}
	now := time.Now()
	return &rateLimiter{rate: rate, burst: float64(burst), tokens: float64(burst), last: now}
}
func waitBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
