package channel

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) Get(id string) (domain.Channel, error) {
	channel, ok := r.registry.Get(id)
	if !ok {
		return nil, ErrChannelNotFound
	}
	return channel, nil
}
