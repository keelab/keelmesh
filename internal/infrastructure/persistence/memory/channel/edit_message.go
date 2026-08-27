package channel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) EditMessage(ctx context.Context, channelID, targetID, messageID, content, state string, metadata map[string]string) error {
	lifecycleKey := strings.Join([]string{channelID, targetID, messageID}, "\x1e")
	state = strings.TrimSpace(state)
	r.mu.RLock()
	entry := r.lifecycle[lifecycleKey]
	r.mu.RUnlock()
	if entry.state != "" && state != "" && state != entry.state {
		return fmt.Errorf("channelcore: message %q lifecycle is already %s", messageID, entry.state)
	}
	channel, err := r.Get(channelID)
	if err != nil {
		if state == domain.MessageLifecycleFinal || state == domain.MessageLifecycleFailed {
			_, fallbackErr := r.Send(ctx, domain.Outbound{
				ChannelID:        channelID,
				TargetID:         targetID,
				ReplyToMessageID: messageID,
				Content:          content,
				Metadata:         metadata,
			})
			if fallbackErr == nil {
				return nil
			}
			return fmt.Errorf("channelcore: edit message %q failed; fallback send failed: %w", messageID, errors.Join(err, fallbackErr))
		}
		return err
	}
	if editor, ok := channel.(domain.LifecycleEditor); ok {
		err = editor.EditMessageWithState(ctx, targetID, messageID, content, state, metadata)
	} else {
		editor, ok := channel.(domain.MessageEditor)
		if !ok {
			err = fmt.Errorf("channelcore: channel %q does not support message editing", channelID)
		} else {
			err = editor.EditMessage(ctx, targetID, messageID, content)
		}
	}
	if err != nil {
		return err
	}
	if state == domain.MessageLifecycleFinal || state == domain.MessageLifecycleFailed {
		r.mu.Lock()
		r.lifecycle[lifecycleKey] = lifecycleEntry{state: state, createdAt: time.Now().UTC()}
		r.mu.Unlock()
	}
	return nil
}
