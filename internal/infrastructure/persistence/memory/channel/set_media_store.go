package channel

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) SetMediaStore(store domain.MediaRepository) {
	r.mu.Lock()
	r.mediaStore = store
	r.mu.Unlock()
}
