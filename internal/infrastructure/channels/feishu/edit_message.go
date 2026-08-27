package feishu

import (
	"context"
	"errors"
	"fmt"
	"strings"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func (c *Channel) EditMessage(ctx context.Context, targetID, messageID, content string) error {
	if !c.Running() {
		return errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(messageID) == "" {
		return errors.New("feishu: message id is required")
	}
	encoded, err := buildMarkdownCard(content)
	if err != nil {
		return err
	}
	response, err := c.client.Im.V1.Message.Patch(ctx, larkim.NewPatchMessageReqBuilder().MessageId(messageID).Body(larkim.NewPatchMessageReqBodyBuilder().Content(encoded).Build()).Build())
	if err != nil {
		return fmt.Errorf("feishu: patch message: %w", err)
	}
	if !response.Success() {
		return fmt.Errorf("feishu: patch message failed: code=%d message=%s", response.Code, response.Msg)
	}
	_ = targetID
	return nil
}
