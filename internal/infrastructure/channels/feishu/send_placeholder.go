package feishu

import (
	"context"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) SendPlaceholder(ctx context.Context, targetID, replyTo, content string) (string, error) {
	receipt, err := c.Send(ctx, domain.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	if err != nil {
		return "", err
	}
	return receipt.MessageID, err
}
