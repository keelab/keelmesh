package channel

import (
	"context"
	"errors"
	"fmt"
)

func (r *Repository) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var failures []error
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	janitorDone := r.janitorDone
	workers := make([]*channelWorker, 0, len(r.workers))
	for id, worker := range r.workers {
		close(worker.stop)
		workers = append(workers, worker)
		delete(r.workers, id)
	}
	for id, sub := range r.subs {
		close(sub.done)
		delete(r.subs, id)
	}
	r.mu.Unlock()
	if janitorDone != nil {
		<-janitorDone
	}
	for _, worker := range workers {
		done := make(chan struct{})
		go func(w *channelWorker) { w.wg.Wait(); close(done) }(worker)
		select {
		case <-done:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
		}
	}
	for _, channel := range r.registry.All() {
		if err := channel.Stop(ctx); err != nil {
			failures = append(failures, fmt.Errorf("stop channel %q: %w", channel.Definition().ID, err))
		}
	}
	return errors.Join(failures...)
}
