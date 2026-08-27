package channel

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/platform/clock"
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
	forwarder := r.inboundForwarder
	forwardQueue := r.forwardQueue
	r.mu.Unlock()
	go r.runJanitor(runCtx)
	if forwarder != nil && forwardQueue != nil {
		r.forwardWG.Add(1)
		go r.runInboundForwarder(runCtx, forwardQueue, forwarder)
	}
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

func (r *Repository) runInboundForwarder(ctx context.Context, queue <-chan domain.Inbound, forward func(context.Context, domain.Inbound) error) {
	defer r.forwardWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-queue:
			if err := forward(ctx, message); err != nil {
				r.forwardFailures.Add(1)
			}
		}
	}
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
			for id, entry := range r.lifecycle {
				if now.Sub(entry.createdAt) > 30*time.Minute {
					delete(r.lifecycle, id)
				}
			}
			for id, entry := range r.idempotency {
				if now.Sub(entry.createdAt) > 30*time.Minute {
					delete(r.idempotency, id)
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
		case message, ok := <-worker.text:
			if !ok {
				return
			}
			for _, chunk := range splitOutbound(message, maxMessageLength(worker.channel.Definition().Kind)) {
				if err := r.sendWithRetry(ctx, worker, chunk); err != nil {
					break
				}
			}
		case message, ok := <-worker.media:
			if !ok {
				return
			}
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
func (r *Repository) sendWithRetry(ctx context.Context, worker *channelWorker, message domain.Outbound) error {
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliverySending, clock.UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, clock.UTC(), err.Error())
			return err
		}
		if receipt, err := worker.channel.Send(ctx, message); err == nil {
			id := message.ID
			if receipt.MessageID != "" {
				id = receipt.MessageID
			}
			r.recordDeliveryWithMessageID(message.ID, message.ChannelID, id, domain.DeliveryAcknowledged, clock.UTC(), "")
			return nil
		} else if !retryable(err) || attempt == maxRetries {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryFailed, clock.UTC(), err.Error())
			return err
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, clock.UTC(), err.Error())
			return err
		}
	}
	return nil
}
func (r *Repository) sendMediaWithRetry(ctx context.Context, worker *channelWorker, channel domain.MediaChannel, message domain.OutboundMedia) {
	r.recordDelivery(message.ID, message.ChannelID, domain.DeliverySending, clock.UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, clock.UTC(), err.Error())
			return
		}
		if receipt, err := channel.SendMedia(ctx, message); err == nil {
			id := message.ID
			if receipt.MessageID != "" {
				id = receipt.MessageID
			}
			r.recordDeliveryWithMessageID(message.ID, message.ChannelID, id, domain.DeliveryAcknowledged, clock.UTC(), "")
			return
		} else if !retryable(err) || attempt == maxRetries {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryFailed, clock.UTC(), err.Error())
			return
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, domain.DeliveryCancelled, clock.UTC(), err.Error())
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
	r.recordDeliveryWithMessageID(id, channelID, id, state, updated, messageErr)
}

func (r *Repository) recordDeliveryWithMessageID(id, channelID, messageID string, state domain.DeliveryState, updated time.Time, messageErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := r.deliveries[id]
	status.MessageID, status.ChannelID, status.State, status.UpdatedAt = messageID, channelID, state, updated
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
			_ = r.sendWithRetry(ctx, worker, message)
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

func maxMessageLength(kind string) int {
	switch kind {
	case "telegram":
		return 4000
	case "dingtalk":
		return 20000
	case "wecom", "wecom_app", "wecom_aibot":
		return 2048
	default:
		return 0
	}
}

func splitOutbound(message domain.Outbound, maxLength int) []domain.Outbound {
	if maxLength <= 0 || len([]rune(message.Content)) <= maxLength {
		return []domain.Outbound{message}
	}

	runes := []rune(message.Content)
	chunks := make([]domain.Outbound, 0, (len(runes)+maxLength-1)/maxLength)
	for len(runes) > 0 {
		end := min(len(runes), maxLength)
		if end < len(runes) {
			for index := end; index > 0; index-- {
				if runes[index-1] != '\n' && runes[index-1] != ' ' && runes[index-1] != '\t' {
					continue
				}
				if end-index < maxLength/4 {
					continue
				}
				end = index
				break
			}
		}
		chunk := message
		chunk.Content = string(runes[:end])
		chunks = append(chunks, chunk)
		runes = runes[end:]
		for len(runes) > 0 && (runes[0] == ' ' || runes[0] == '\t' || runes[0] == '\n' || runes[0] == '\r') {
			runes = runes[1:]
		}
	}
	return chunks
}
