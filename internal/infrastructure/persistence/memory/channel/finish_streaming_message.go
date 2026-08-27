package channel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) FinishStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	controller, ok := channel.(domain.StreamingController)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	if err := r.ensureStreamingOpen(channelID, targetID, messageID); err != nil {
		return err
	}
	if err := controller.FinishStreamingMessage(ctx, targetID, messageID, content); err != nil {
		return err
	}
	key := strings.Join([]string{channelID, targetID, messageID}, "\x1e")
	r.mu.Lock()
	r.lifecycle[key] = lifecycleEntry{state: domain.MessageLifecycleFinal, createdAt: time.Now().UTC()}
	r.mu.Unlock()
	return nil
}
