package channel

import (
	"fmt"
	"strings"
	"sync"

	"github.com/keelab/keelmesh/internal/domain"
)

type Registry struct {
	mu       sync.RWMutex
	channels map[string]domain.Channel
}

func NewRegistry() *Registry {
	return &Registry{
		channels: make(map[string]domain.Channel),
	}
}

func (r *Registry) Register(channel domain.Channel) error {
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

func (r *Registry) Get(id string) (domain.Channel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channel, ok := r.channels[id]
	return channel, ok
}

func (r *Registry) All() []domain.Channel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	channels := make([]domain.Channel, 0, len(r.channels))
	for _, channel := range r.channels {
		channels = append(channels, channel)
	}
	return channels
}
