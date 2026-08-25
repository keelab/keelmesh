package channel

import (
	"context"
	"errors"
	"io"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) StoreMedia(ctx context.Context, filename, contentType string, content io.Reader) (domain.MediaPartEntity, error) {
	r.mu.RLock()
	store := r.mediaStore
	r.mu.RUnlock()
	if store == nil {
		return domain.MediaPartEntity{}, errors.New("channelcore: media store is not configured")
	}
	return store.Store(ctx, filename, contentType, content)
}
