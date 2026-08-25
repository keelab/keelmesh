package telegram

import (
	"context"
	"strconv"
)

type sentMessage struct {
	MessageID int `json:"message_id"`
}

func (c *Channel) EditMessage(ctx context.Context, targetID, messageID, content string) error {
	id, err := strconv.Atoi(messageID)
	if err != nil {
		return err
	}
	var result sentMessage
	return c.call(ctx, "editMessageText", map[string]any{"chat_id": targetID, "message_id": id, "text": content, "parse_mode": "HTML"}, &result)
}
