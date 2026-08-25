package feishu

import (
	"context"
)

func (c *Channel) EditMessageWithState(ctx context.Context, targetID, messageID, content, _ string, _ map[string]string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}
