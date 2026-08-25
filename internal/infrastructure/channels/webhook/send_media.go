package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

type envelope struct {
	MessageID  string                   `json:"message_id"`
	ChatID     string                   `json:"chat_id"`
	SenderID   string                   `json:"sender_id"`
	SenderName string                   `json:"sender_name"`
	Content    string                   `json:"content"`
	Media      []domain.MediaPartEntity `json:"media"`
	Metadata   map[string]string        `json:"metadata"`
	Timestamp  string                   `json:"timestamp"`
}

func (c *Channel) SendMedia(ctx context.Context, msg domain.OutboundMedia) (domain.ReceiptEntity, error) {
	if strings.TrimSpace(c.config.OutboundURL) == "" {
		return domain.ReceiptEntity{}, errors.New("webhook: outbound_url is not configured")
	}
	body, _ := json.Marshal(struct {
		Envelope envelope                 `json:"message"`
		Parts    []domain.MediaPartEntity `json:"media"`
	}{Envelope: envelope{MessageID: msg.ID, ChatID: msg.TargetID, Metadata: msg.Metadata}, Parts: msg.Parts})
	if err := c.post(ctx, c.config.OutboundURL, body); err != nil {
		return domain.ReceiptEntity{}, err
	}
	return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
