package feishu

import (
	"context"
)

func (c *Channel) FinishStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}
