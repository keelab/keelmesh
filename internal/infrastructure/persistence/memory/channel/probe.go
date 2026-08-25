package channel

import (
	"context"
	"time"
)

func (r *Repository) Probe(ctx context.Context, id string) (time.Duration, error) {
	channel, err := r.Get(id)
	if err != nil {
		return 0, err
	}
	started := time.Now()
	err = channel.Probe(ctx)
	return time.Since(started), err
}
