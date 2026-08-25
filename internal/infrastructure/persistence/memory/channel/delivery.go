package channel

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) Delivery(id string) (domain.DeliveryStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, ok := r.deliveries[id]
	return status, ok
}
