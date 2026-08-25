package channel

import (
	"context"
	"errors"
)

func (r *Repository) ReleaseMedia(ctx context.Context, ref string) error {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return errors.New("channelcore: media store is not configured")
	}
	return store.Release(ctx, ref)
}
