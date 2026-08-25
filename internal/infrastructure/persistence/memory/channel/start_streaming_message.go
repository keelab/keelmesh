package channel

import (
	"context"
	"fmt"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) StartStreamingMessage(ctx context.Context, channelID, targetID, replyTo, content string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(domain.StreamingController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.StartStreamingMessage(ctx, targetID, replyTo, content)
}
