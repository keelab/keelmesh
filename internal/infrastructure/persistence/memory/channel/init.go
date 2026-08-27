package channel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

var (
	ErrChannelNotFound = errors.New("channelcore: channel not found")
	ErrChannelDisabled = errors.New("channelcore: channel is disabled")
	ErrInvalidMessage  = errors.New("channelcore: invalid message")
	ErrQueueFull       = errors.New("channelcore: channel queue is full")
	ErrUnsupported     = errors.New("channelcore: capability unsupported")
)

type Repository struct {
	registry           *Registry
	mu                 sync.RWMutex
	nextID             uint64
	subs               map[uint64]*subscription
	workers            map[string]*channelWorker
	deliveries         map[string]domain.DeliveryStatus
	idempotency        map[string]idempotencyEntry
	actions            map[string]actionEntry
	reactions          map[string]reactionAction
	lifecycle          map[string]lifecycleEntry
	inboundDropped     atomic.Uint64
	cancel             context.CancelFunc
	janitorDone        chan struct{}
	mediaStore         domain.MediaDomain
	inboundForwarder   func(context.Context, domain.Inbound) error
	outboundAuthorizer func(context.Context, domain.Outbound) error
	forwardQueue       chan domain.Inbound
	forwardWG          sync.WaitGroup
	forwardDropped     atomic.Uint64
	forwardFailures    atomic.Uint64
}
type subscription struct {
	channels map[string]struct{}
	queue    chan domain.Inbound
	done     chan struct{}
}

// SetInboundForwarder attaches the GateCore ingress boundary. A nil forwarder
// keeps the repository local-only, which is useful for standalone operation.
func (r *Repository) SetInboundForwarder(forwarder func(context.Context, domain.Inbound) error) {
	r.mu.Lock()
	r.inboundForwarder = forwarder
	if forwarder != nil && r.forwardQueue == nil {
		r.forwardQueue = make(chan domain.Inbound, 64)
	}
	r.mu.Unlock()
}

// SetOutboundAuthorizer attaches the GateCore outbound policy boundary.
func (r *Repository) SetOutboundAuthorizer(authorizer func(context.Context, domain.Outbound) error) {
	r.mu.Lock()
	r.outboundAuthorizer = authorizer
	r.mu.Unlock()
}

type channelWorker struct {
	channel domain.Channel
	text    chan domain.Outbound
	media   chan domain.OutboundMedia
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

type actionEntry struct {
	stop      func()
	createdAt time.Time
}
type reactionAction struct {
	complete  func()
	expire    func()
	createdAt time.Time
}
type lifecycleEntry struct {
	state     string
	createdAt time.Time
}
type idempotencyEntry struct {
	messageID string
	createdAt time.Time
}

func NewRepository(registry *Registry) (*Repository, error) {
	if registry == nil {
		return nil, fmt.Errorf("channelcore: registry is nil")
	}
	return &Repository{
		registry:    registry,
		subs:        make(map[uint64]*subscription),
		workers:     make(map[string]*channelWorker),
		deliveries:  make(map[string]domain.DeliveryStatus),
		idempotency: make(map[string]idempotencyEntry),
		actions:     make(map[string]actionEntry),
		reactions:   make(map[string]reactionAction),
		lifecycle:   make(map[string]lifecycleEntry),
	}, nil
}

func (r *Repository) SendNotification(ctx context.Context, message domain.Notification) (domain.NotificationReceipt, error) {
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return domain.NotificationReceipt{}, err
	}
	if controller, ok := channel.(domain.NotificationController); ok {
		return controller.SendNotification(ctx, message)
	}
	metadata := make(map[string]string, len(message.Metadata)+2)
	for key, value := range message.Metadata {
		metadata[key] = value
	}
	if message.MentionAll {
		metadata["mention_all"] = "true"
	}
	if len(message.MentionIDs) > 0 {
		metadata["mention_ids"] = strings.Join(message.MentionIDs, ",")
	}
	receipt, err := r.Send(ctx, domain.Outbound{
		ID:             message.IdempotencyKey,
		ChannelID:      message.ChannelID,
		TargetID:       message.TargetID,
		Content:        message.Content,
		Metadata:       metadata,
		IdempotencyKey: message.IdempotencyKey,
	})
	if err != nil {
		return domain.NotificationReceipt{}, err
	}
	return domain.NotificationReceipt{
		ReceiptEntity: receipt,
	}, nil
}

func (r *Repository) PrepareInbound(ctx context.Context, message domain.InboundPreparation) (domain.InboundPreparationReceipt, error) {
	var receipt domain.InboundPreparationReceipt
	var cleanup []func()
	if message.TargetID != "" {
		if actionID, err := r.StartTyping(ctx, message.ChannelID, message.TargetID); err == nil {
			receipt.TypingActionID = actionID
			cleanup = append(cleanup, func() { r.StopTyping(actionID) })
		} else if !errors.Is(err, ErrUnsupported) {
			return receipt, err
		}
	}
	if message.Reaction != "" && message.MessageID != "" && message.TargetID != "" {
		if actionID, err := r.ReactToMessage(ctx, message.ChannelID, message.TargetID, message.MessageID, message.Reaction); err == nil {
			receipt.ReactionActionID = actionID
			cleanup = append(cleanup, func() { r.ExpireReaction(actionID) })
		} else if !errors.Is(err, ErrUnsupported) {
			for _, stop := range cleanup {
				stop()
			}
			return receipt, err
		}
	}
	if message.PlaceholderContent != "" && message.TargetID != "" {
		placeholderID, err := r.SendPlaceholder(ctx, message.ChannelID, message.TargetID, message.ReplyToMessageID, message.PlaceholderContent)
		if err != nil && !errors.Is(err, ErrUnsupported) {
			for _, stop := range cleanup {
				stop()
			}
			return receipt, err
		}
		receipt.PlaceholderMessageID = placeholderID
	}
	return receipt, nil
}
