package telegram

import (
	"context"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) SendPlaceholder(ctx context.Context, targetID, replyTo, content string) (string, error) {
	if content == "" {
		content = c.config.PlaceholderText
	}
	if content == "" {
		content = "正在处理，请稍候..."
	}
	receipt, err := c.Send(ctx, domain.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	return receipt.MessageID, err
}
