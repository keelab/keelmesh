package feishu

import (
	"context"
	"fmt"
	"strings"

	"github.com/keelab/keelmesh/internal/domain"
)

// SendNotification preserves Feishu's native mention markup. Urgency actions
// are intentionally rejected until their receiver identity type is part of the
// public ChannelCore contract; silently treating an urgent request as ordinary
// delivery would produce a false receipt.
func (c *Channel) SendNotification(ctx context.Context, message domain.Notification) (domain.NotificationReceipt, error) {
	if strings.TrimSpace(message.Urgency) != "" && message.Urgency != "none" {
		return domain.NotificationReceipt{}, fmt.Errorf("feishu: urgency %q is unsupported", message.Urgency)
	}
	content := composeNotificationContent(message.Content, message.MentionIDs, message.MentionAll)
	receipt, err := c.Send(ctx, domain.Outbound{
		ChannelID:      message.ChannelID,
		TargetID:       message.TargetID,
		Content:        content,
		IdempotencyKey: message.IdempotencyKey,
		Metadata:       message.Metadata,
	})
	if err != nil {
		return domain.NotificationReceipt{}, err
	}
	return domain.NotificationReceipt{ReceiptEntity: receipt}, nil
}

func composeNotificationContent(content string, mentionIDs []string, mentionAll bool) string {
	mentions := make([]string, 0, len(mentionIDs)+1)
	for _, id := range mentionIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			mentions = append(mentions, "<at user_id=\""+id+"\"></at>")
		}
	}
	if mentionAll {
		mentions = append(mentions, "<at user_id=\"all\"></at>")
	}
	if len(mentions) == 0 {
		return strings.TrimSpace(content)
	}
	return strings.Join(mentions, " ") + "\n" + strings.TrimSpace(content)
}
