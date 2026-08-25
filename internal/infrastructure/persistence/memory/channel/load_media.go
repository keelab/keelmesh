package channel

import (
	"context"
	"errors"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) LoadMedia(ctx context.Context, ref string) (domain.MediaEntity, error) {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return domain.MediaEntity{}, errors.New("channelcore: media store is not configured")
	}
	return store.Open(ctx, ref)
}
