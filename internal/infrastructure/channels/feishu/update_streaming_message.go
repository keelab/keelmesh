package feishu

import (
	"context"
)

func (c *Channel) UpdateStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}
