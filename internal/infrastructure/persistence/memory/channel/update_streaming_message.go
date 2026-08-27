package channel

import (
	"context"
	"fmt"
	"strings"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) UpdateStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	if err := r.ensureStreamingOpen(channelID, targetID, messageID); err != nil {
		return err
	}
	controller, ok := channel.(domain.StreamingController)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.UpdateStreamingMessage(ctx, targetID, messageID, content)
}

func (r *Repository) ensureStreamingOpen(channelID, targetID, messageID string) error {
	key := strings.Join([]string{channelID, targetID, messageID}, "\x1e")
	r.mu.RLock()
	entry := r.lifecycle[key]
	r.mu.RUnlock()
	if entry.state == domain.MessageLifecycleFinal || entry.state == domain.MessageLifecycleFailed {
		return fmt.Errorf("channelcore: message %q lifecycle is already %s", messageID, entry.state)
	}
	return nil
}
