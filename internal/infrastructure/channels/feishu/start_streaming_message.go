package feishu

import (
	"context"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) StartStreamingMessage(ctx context.Context, targetID, replyTo, content string) (string, error) {
	receipt, err := c.Send(ctx, domain.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	return receipt.MessageID, err
}
