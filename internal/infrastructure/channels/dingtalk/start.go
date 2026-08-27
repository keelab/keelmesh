package dingtalk

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
)

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("dingtalk: sink is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.sink = sink
	c.mu.Unlock()
	c.stream = client.NewStreamClient(client.WithAppCredential(client.NewAppCredentialConfig(c.config.ClientID, c.config.ClientSecret)), client.WithAutoReconnect(true))
	c.stream.RegisterChatBotCallbackRouter(c.receive)
	if err := c.stream.Start(runCtx); err != nil {
		return err
	}
	c.running.Store(true)
	return nil
}

func (c *Channel) receive(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	content := data.Text.Content
	if content == "" {
		if m, ok := data.Content.(map[string]any); ok {
			if v, ok := m["content"].(string); ok {
				content = v
			}
		}
	}
	if strings.TrimSpace(content) == "" {
		return nil, nil
	}
	sender := data.SenderStaffId
	chatID := sender
	if data.ConversationType != "1" {
		chatID = data.ConversationId
	}
	if !allowed(c.config.AllowFrom, sender) {
		return nil, nil
	}
	c.webhooks.Store(chatID, data.SessionWebhook)
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		metadata := map[string]string{
			"conversation_id":   data.ConversationId,
			"conversation_type": data.ConversationType,
			"session_webhook":   data.SessionWebhook,
			"scope":             "direct",
		}
		if data.ConversationType != "1" {
			metadata["scope"] = "group"
			metadata["mentioned"] = "true"
		}
		sink(domain.Inbound{ChannelID: c.config.ID, ChatID: chatID, SenderID: sender, SenderName: data.SenderNick, Content: strings.TrimSpace(content), Metadata: metadata, ReceivedAt: time.Now().UTC()})
	}
	return nil, nil
}
func allowed(list []string, id string) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if strings.TrimPrefix(strings.TrimSpace(v), "@") == id {
			return true
		}
	}
	return false
}
