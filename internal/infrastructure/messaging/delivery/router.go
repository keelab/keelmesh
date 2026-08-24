package delivery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/keelab/keelith/outbox"
)

var (
	ErrDestinationInvalid       = errors.New("delivery: destination is invalid")
	ErrDestinationNotRegistered = errors.New("delivery: destination is not registered")
)

// Router dispatches durable Outbox messages to destination-specific publishers.
type Router struct {
	mu         sync.RWMutex
	publishers map[string]outbox.Publisher
}

func NewRouter() *Router {
	return &Router{publishers: make(map[string]outbox.Publisher)}
}

func (r *Router) Register(destination string, publisher outbox.Publisher) error {
	destination = strings.TrimSpace(destination)
	if destination == "" || publisher == nil {
		return ErrDestinationInvalid
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.publishers[destination]; exists {
		return fmt.Errorf("%w: %q is already registered", ErrDestinationInvalid, destination)
	}
	r.publishers[destination] = publisher
	return nil
}

func (r *Router) Publish(ctx context.Context, message outbox.Message) error {
	if r == nil {
		return ErrDestinationNotRegistered
	}
	r.mu.RLock()
	publisher, ok := r.publishers[message.Destination]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrDestinationNotRegistered, message.Destination)
	}
	if err := publisher.Publish(ctx, message); err != nil {
		return fmt.Errorf("deliver %q: %w", message.Destination, err)
	}
	return nil
}
