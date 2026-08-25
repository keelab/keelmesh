package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

var (
	ErrChannelNotFound = errors.New("channelcore: channel not found")
	ErrChannelDisabled = errors.New("channelcore: channel is disabled")
	ErrInvalidMessage  = errors.New("channelcore: invalid message")
	ErrQueueFull       = errors.New("channelcore: channel queue is full")
)

type Repository struct {
	registry    *Registry
	mu          sync.RWMutex
	nextID      uint64
	subs        map[uint64]*subscription
	workers     map[string]*channelWorker
	deliveries  map[string]domain.DeliveryStatus
	actions     map[string]actionEntry
	reactions   map[string]reactionAction
	cancel      context.CancelFunc
	janitorDone chan struct{}
	mediaStore  domain.MediaDomain
}
type subscription struct {
	channels map[string]struct{}
	queue    chan domain.Inbound
	done     chan struct{}
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

func NewRepository(registry *Registry) (*Repository, error) {
	if registry == nil {
		return nil, fmt.Errorf("channelcore: registry is nil")
	}
	return &Repository{
		registry:   registry,
		subs:       make(map[uint64]*subscription),
		workers:    make(map[string]*channelWorker),
		deliveries: make(map[string]domain.DeliveryStatus),
		actions:    make(map[string]actionEntry),
		reactions:  make(map[string]reactionAction),
	}, nil
}
