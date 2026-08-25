package channel

import (
	"context"
	"fmt"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) EditMessage(ctx context.Context, channelID, targetID, messageID, content, state string, metadata map[string]string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	if editor, ok := channel.(domain.LifecycleEditor); ok {
		return editor.EditMessageWithState(ctx, targetID, messageID, content, state, metadata)
	}
	editor, ok := channel.(domain.MessageEditor)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support message editing", channelID)
	}
	return editor.EditMessage(ctx, targetID, messageID, content)
}
