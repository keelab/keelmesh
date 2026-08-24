package channelcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

var (
	ErrChannelNotFound = errors.New("channelcore: channel not found")
	ErrChannelDisabled = errors.New("channelcore: channel is disabled")
	ErrInvalidMessage  = errors.New("channelcore: invalid message")
	ErrQueueFull       = errors.New("channelcore: channel queue is full")
)

type Definition struct {
	ID            string
	Kind          string
	Enabled       bool
	Capabilities  []string
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}

type Inbound struct {
	ChannelID  string
	MessageID  string
	ChatID     string
	SenderID   string
	SenderName string
	Content    string
	Metadata   map[string]string
	Media      []MediaPart
	ReceivedAt time.Time
}

type MediaPart struct {
	Type        string
	Ref         string
	Caption     string
	Filename    string
	ContentType string
}

type OutboundMedia struct {
	ID             string
	ChannelID      string
	TargetID       string
	Parts          []MediaPart
	Metadata       map[string]string
	IdempotencyKey string
}

type Outbound struct {
	ID               string
	ChannelID        string
	TargetID         string
	ReplyToMessageID string
	Content          string
	Metadata         map[string]string
	IdempotencyKey   string
}

type Receipt struct {
	MessageID  string
	AcceptedAt time.Time
	State      DeliveryState
}

type DeliveryState string

const (
	DeliveryQueued       DeliveryState = "queued"
	DeliverySending      DeliveryState = "sending"
	DeliveryAcknowledged DeliveryState = "acknowledged"
	DeliveryFailed       DeliveryState = "failed"
	DeliveryCancelled    DeliveryState = "cancelled"
)

type DeliveryStatus struct {
	MessageID  string
	ChannelID  string
	State      DeliveryState
	AcceptedAt time.Time
	UpdatedAt  time.Time
	Error      string
}

type Sink func(Inbound)

// Channel is implemented by each concrete platform adapter. The adapter owns
// platform connections and translates platform messages into channelcore types.
type Channel interface {
	Definition() Definition
	Start(context.Context, Sink) error
	Stop(context.Context) error
	Send(context.Context, Outbound) (Receipt, error)
	Probe(context.Context) error
	Running() bool
}

type MediaChannel interface {
	SendMedia(context.Context, OutboundMedia) (Receipt, error)
}
type MessageEditor interface {
	EditMessage(context.Context, string, string, string) error
}
type LifecycleEditor interface {
	EditMessageWithState(context.Context, string, string, string, string, map[string]string) error
}
type TypingController interface {
	StartTyping(context.Context, string) (func(), error)
}
type ReactionController interface {
	ReactToMessage(context.Context, string, string, string) (func(), func(), error)
}
type PlaceholderController interface {
	SendPlaceholder(context.Context, string, string, string) (string, error)
}
type StreamingController interface {
	StartStreamingMessage(context.Context, string, string, string) (string, error)
	UpdateStreamingMessage(context.Context, string, string, string) error
	FinishStreamingMessage(context.Context, string, string, string) error
}

type Registry struct {
	mu       sync.RWMutex
	channels map[string]Channel
}

func NewRegistry() *Registry { return &Registry{channels: make(map[string]Channel)} }

func (r *Registry) Register(channel Channel) error {
	if r == nil || channel == nil {
		return fmt.Errorf("%w: channel", ErrInvalidMessage)
	}
	definition := channel.Definition()
	id := strings.TrimSpace(definition.ID)
	if id == "" || id != definition.ID {
		return fmt.Errorf("%w: channel id", ErrInvalidMessage)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.channels[id]; exists {
		return fmt.Errorf("channelcore: channel %q already registered", id)
	}
	r.channels[id] = channel
	return nil
}

func (r *Registry) Get(id string) (Channel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channel, ok := r.channels[id]
	return channel, ok
}

func (r *Registry) All() []Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channels := make([]Channel, 0, len(r.channels))
	for _, channel := range r.channels {
		channels = append(channels, channel)
	}
	return channels
}

type Runtime struct {
	registry    *Registry
	mu          sync.RWMutex
	nextID      uint64
	subs        map[uint64]*subscription
	workers     map[string]*channelWorker
	deliveries  map[string]DeliveryStatus
	actions     map[string]actionEntry
	reactions   map[string]reactionAction
	cancel      context.CancelFunc
	janitorDone chan struct{}
	mediaStore  MediaStore
}

type actionEntry struct {
	stop      func()
	createdAt time.Time
}

type reactionAction struct {
	complete  func()
	expire    func()
	createdAt time.Time
}

type channelWorker struct {
	channel Channel
	text    chan Outbound
	media   chan OutboundMedia
	stop    chan struct{}
	limiter *rateLimiter
	wg      sync.WaitGroup
}

type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
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

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

type subscription struct {
	channels map[string]struct{}
	queue    chan Inbound
	done     chan struct{}
}

func NewRuntime(registry *Registry) (*Runtime, error) {
	if registry == nil {
		return nil, fmt.Errorf("channelcore: registry is nil")
	}
	return &Runtime{registry: registry, subs: make(map[uint64]*subscription), workers: make(map[string]*channelWorker), deliveries: make(map[string]DeliveryStatus), actions: make(map[string]actionEntry), reactions: make(map[string]reactionAction)}, nil
}

func (r *Runtime) SetMediaStore(store MediaStore) {
	r.mu.Lock()
	r.mediaStore = store
	r.mu.Unlock()
}

func (r *Runtime) StoreMedia(ctx context.Context, filename, contentType string, content io.Reader) (MediaPart, error) {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return MediaPart{}, errors.New("channelcore: media store is not configured")
	}
	return store.Store(ctx, filename, contentType, content)
}

func (r *Runtime) LoadMedia(ctx context.Context, ref string) (MediaResource, error) {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return MediaResource{}, errors.New("channelcore: media store is not configured")
	}
	return store.Open(ctx, ref)
}

func (r *Runtime) ReleaseMedia(ctx context.Context, ref string) error {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return errors.New("channelcore: media store is not configured")
	}
	return store.Release(ctx, ref)
}

func (r *Runtime) Start(ctx context.Context) error {
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
	started := make([]Channel, 0)
	for _, channel := range r.registry.All() {
		if !channel.Definition().Enabled {
			continue
		}
		if err := channel.Start(runCtx, r.Publish); err != nil {
			cancel()
			for i := len(started) - 1; i >= 0; i-- {
				_ = started[i].Stop(context.WithoutCancel(ctx))
			}
			return fmt.Errorf("start channel %q: %w", channel.Definition().ID, err)
		}
		started = append(started, channel)
		definition := channel.Definition()
		queueSize := definition.QueueSize
		if queueSize <= 0 {
			queueSize = 16
		}
		worker := &channelWorker{channel: channel, text: make(chan Outbound, queueSize), media: make(chan OutboundMedia, queueSize), stop: make(chan struct{}), limiter: newRateLimiter(definition.RatePerSecond, definition.Burst)}
		r.mu.Lock()
		r.workers[channel.Definition().ID] = worker
		r.mu.Unlock()
		worker.wg.Add(1)
		go r.runWorker(runCtx, worker)
	}
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var failures []error
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	janitorDone := r.janitorDone
	workers := make([]*channelWorker, 0, len(r.workers))
	for id, worker := range r.workers {
		close(worker.stop)
		workers = append(workers, worker)
		delete(r.workers, id)
	}
	for id, sub := range r.subs {
		close(sub.done)
		delete(r.subs, id)
	}
	r.mu.Unlock()
	if janitorDone != nil {
		<-janitorDone
	}
	for _, worker := range workers {
		done := make(chan struct{})
		go func(w *channelWorker) { w.wg.Wait(); close(done) }(worker)
		select {
		case <-done:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
		}
	}
	for _, channel := range r.registry.All() {
		if err := channel.Stop(ctx); err != nil {
			failures = append(failures, fmt.Errorf("stop channel %q: %w", channel.Definition().ID, err))
		}
	}
	return errors.Join(failures...)
}

func (r *Runtime) List() []Definition {
	channels := r.registry.All()
	definitions := make([]Definition, 0, len(channels))
	for _, channel := range channels {
		definitions = append(definitions, channel.Definition())
	}
	return definitions
}

func (r *Runtime) Get(id string) (Channel, error) {
	channel, ok := r.registry.Get(id)
	if !ok {
		return nil, ErrChannelNotFound
	}
	return channel, nil
}

func (r *Runtime) Send(ctx context.Context, message Outbound) (Receipt, error) {
	if ctx == nil {
		return Receipt{}, ErrInvalidMessage
	}
	if strings.TrimSpace(message.ChannelID) == "" || strings.TrimSpace(message.TargetID) == "" || strings.TrimSpace(message.Content) == "" {
		return Receipt{}, ErrInvalidMessage
	}
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return Receipt{}, err
	}
	if !channel.Definition().Enabled {
		return Receipt{}, ErrChannelDisabled
	}
	r.mu.RLock()
	worker := r.workers[message.ChannelID]
	r.mu.RUnlock()
	if worker == nil {
		return Receipt{}, fmt.Errorf("channelcore: channel %q is not running", message.ChannelID)
	}
	if message.ID == "" {
		message.ID = fmt.Sprintf("channel-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	r.recordDelivery(message.ID, message.ChannelID, DeliveryQueued, now, "")
	select {
	case worker.text <- message:
		return Receipt{MessageID: message.ID, AcceptedAt: now, State: DeliveryQueued}, nil
	default:
		r.deleteDelivery(message.ID)
		return Receipt{}, ErrQueueFull
	}
}

func (r *Runtime) SendMedia(ctx context.Context, message OutboundMedia) (Receipt, error) {
	if ctx == nil {
		return Receipt{}, ErrInvalidMessage
	}
	if strings.TrimSpace(message.ChannelID) == "" || strings.TrimSpace(message.TargetID) == "" || len(message.Parts) == 0 {
		return Receipt{}, ErrInvalidMessage
	}
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return Receipt{}, err
	}
	if !channel.Definition().Enabled {
		return Receipt{}, ErrChannelDisabled
	}
	if _, ok := channel.(MediaChannel); !ok {
		return Receipt{}, fmt.Errorf("channelcore: channel %q does not support media", message.ChannelID)
	}
	r.mu.RLock()
	worker := r.workers[message.ChannelID]
	r.mu.RUnlock()
	if worker == nil {
		return Receipt{}, fmt.Errorf("channelcore: channel %q is not running", message.ChannelID)
	}
	if message.ID == "" {
		message.ID = fmt.Sprintf("media-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	r.recordDelivery(message.ID, message.ChannelID, DeliveryQueued, now, "")
	select {
	case worker.media <- message:
		return Receipt{MessageID: message.ID, AcceptedAt: now, State: DeliveryQueued}, nil
	default:
		r.deleteDelivery(message.ID)
		return Receipt{}, ErrQueueFull
	}
}

func (r *Runtime) runWorker(ctx context.Context, worker *channelWorker) {
	defer worker.wg.Done()
	for {
		select {
		case message := <-worker.text:
			r.sendWithRetry(ctx, worker, message)
		case message := <-worker.media:
			if channel, ok := worker.channel.(MediaChannel); ok {
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

func (r *Runtime) drainWorker(ctx context.Context, worker *channelWorker) {
	for {
		select {
		case message := <-worker.text:
			r.sendWithRetry(ctx, worker, message)
		case message := <-worker.media:
			if channel, ok := worker.channel.(MediaChannel); ok {
				r.sendMediaWithRetry(ctx, worker, channel, message)
			}
		default:
			return
		}
	}
}

func (r *Runtime) sendWithRetry(ctx context.Context, worker *channelWorker, message Outbound) {
	r.recordDelivery(message.ID, message.ChannelID, DeliverySending, time.Now().UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
		if receipt, err := worker.channel.Send(ctx, message); err == nil {
			id := message.ID
			if receipt.MessageID != "" {
				id = receipt.MessageID
			}
			r.recordDelivery(message.ID, message.ChannelID, DeliveryAcknowledged, time.Now().UTC(), "")
			_ = id
			return
		} else if attempt == maxRetries-1 {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryFailed, time.Now().UTC(), err.Error())
			return
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
	}
}

func (r *Runtime) sendMediaWithRetry(ctx context.Context, worker *channelWorker, channel MediaChannel, message OutboundMedia) {
	r.recordDelivery(message.ID, message.ChannelID, DeliverySending, time.Now().UTC(), "")
	maxRetries := worker.channel.Definition().MaxRetries
	if maxRetries <= 0 {
		maxRetries = 4
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := worker.limiter.wait(ctx); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
		if _, err := channel.SendMedia(ctx, message); err == nil {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryAcknowledged, time.Now().UTC(), "")
			return
		} else if attempt == maxRetries-1 {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryFailed, time.Now().UTC(), err.Error())
			return
		}
		if err := waitBackoff(ctx, time.Duration(1<<attempt)*250*time.Millisecond); err != nil {
			r.recordDelivery(message.ID, message.ChannelID, DeliveryCancelled, time.Now().UTC(), err.Error())
			return
		}
	}
}

func (r *Runtime) recordDelivery(id, channelID string, state DeliveryState, updated time.Time, messageErr string) {
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

func (r *Runtime) deleteDelivery(id string) {
	r.mu.Lock()
	delete(r.deliveries, id)
	r.mu.Unlock()
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

func (r *Runtime) Delivery(id string) (DeliveryStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, ok := r.deliveries[id]
	return status, ok
}

func (r *Runtime) runJanitor(ctx context.Context) {
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

func (r *Runtime) Probe(ctx context.Context, id string) (time.Duration, error) {
	channel, err := r.Get(id)
	if err != nil {
		return 0, err
	}
	started := time.Now()
	err = channel.Probe(ctx)
	return time.Since(started), err
}

func (r *Runtime) Publish(message Inbound) {
	if message.ReceivedAt.IsZero() {
		message.ReceivedAt = time.Now().UTC()
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

func (r *Runtime) Subscribe(ctx context.Context, channelIDs []string) (<-chan Inbound, func(), error) {
	if ctx == nil {
		return nil, nil, ErrInvalidMessage
	}
	filters := make(map[string]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		if _, err := r.Get(id); err != nil {
			return nil, nil, err
		}
		filters[id] = struct{}{}
	}
	r.mu.Lock()
	r.nextID++
	id := r.nextID
	sub := &subscription{channels: filters, queue: make(chan Inbound, 64), done: make(chan struct{})}
	r.subs[id] = sub
	r.mu.Unlock()
	cancel := func() {
		r.mu.Lock()
		if current, ok := r.subs[id]; ok {
			close(current.done)
			delete(r.subs, id)
		}
		r.mu.Unlock()
	}
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return sub.queue, cancel, nil
}
