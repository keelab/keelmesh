package dingtalk

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
)

func (c *Channel) Send(ctx context.Context, msg domain.Outbound) (domain.ReceiptEntity, error) {
	raw, ok := c.webhooks.Load(msg.TargetID)
	if !ok {
		return domain.ReceiptEntity{}, fmt.Errorf("dingtalk: no session webhook for %q", msg.TargetID)
	}
	hook, ok := raw.(string)
	if !ok || hook == "" {
		return domain.ReceiptEntity{}, errors.New("dingtalk: invalid session webhook")
	}
	replier := chatbot.NewChatbotReplier()
	if err := replier.SimpleReplyMarkdown(ctx, hook, []byte("channelcore"), []byte(msg.Content)); err != nil {
		return domain.ReceiptEntity{}, err
	}
	return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
