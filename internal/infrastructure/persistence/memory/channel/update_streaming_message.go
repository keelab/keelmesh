package channel

import (
	"context"
	"fmt"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) UpdateStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	controller, ok := channel.(domain.StreamingController)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.UpdateStreamingMessage(ctx, targetID, messageID, content)
}
