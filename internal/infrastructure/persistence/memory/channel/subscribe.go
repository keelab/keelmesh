package channel

import (
	"context"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) Subscribe(ctx context.Context, channelIDs []string) (<-chan domain.Inbound, func(), error) {
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
	sub := &subscription{channels: filters, queue: make(chan domain.Inbound, 64), done: make(chan struct{})}
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
