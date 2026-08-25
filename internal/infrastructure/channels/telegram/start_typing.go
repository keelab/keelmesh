package telegram

import (
	"context"
	"time"
)

func (c *Channel) StartTyping(ctx context.Context, targetID string) (func(), error) {
	if err := c.call(ctx, "sendChatAction", map[string]any{"chat_id": targetID, "action": "typing"}, &struct{}{}); err != nil {
		return nil, err
	}
	typingCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-typingCtx.Done():
				return
			case <-ticker.C:
				_ = c.call(typingCtx, "sendChatAction", map[string]any{"chat_id": targetID, "action": "typing"}, &struct{}{})
			}
		}
	}()
	return cancel, nil
}
