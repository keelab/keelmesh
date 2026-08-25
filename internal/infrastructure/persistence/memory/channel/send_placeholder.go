package channel

import (
	"context"
	"fmt"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) SendPlaceholder(ctx context.Context, channelID, targetID, replyTo, content string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(domain.PlaceholderController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support placeholders", channelID)
	}
	return controller.SendPlaceholder(ctx, targetID, replyTo, content)
}
