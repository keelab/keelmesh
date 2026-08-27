package feishu

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func (c *Channel) Send(ctx context.Context, message domain.Outbound) (domain.ReceiptEntity, error) {
	if !c.Running() {
		return domain.ReceiptEntity{}, errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(message.TargetID) == "" {
		return domain.ReceiptEntity{}, errors.New("feishu: target id is required")
	}
	payload, err := buildMarkdownCard(message.Content)
	if err != nil {
		return domain.ReceiptEntity{}, err
	}
	var resp *larkim.CreateMessageResp
	if message.ReplyToMessageID != "" {
		replyReq := larkim.NewReplyMessageReqBuilder().MessageId(message.ReplyToMessageID).Body(larkim.NewReplyMessageReqBodyBuilder().MsgType(larkim.MsgTypeInteractive).Content(payload).ReplyInThread(true).Build()).Build()
		replyResp, replyErr := c.client.Im.V1.Message.Reply(ctx, replyReq)
		if replyErr != nil {
			return domain.ReceiptEntity{}, fmt.Errorf("feishu: reply message: %w", replyErr)
		}
		if !replyResp.Success() {
			return domain.ReceiptEntity{}, fmt.Errorf("feishu: reply message failed: code=%d message=%s", replyResp.Code, replyResp.Msg)
		}
		id := ""
		if replyResp.Data != nil && replyResp.Data.MessageId != nil {
			id = *replyResp.Data.MessageId
		}
		return domain.ReceiptEntity{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
	}
	req := larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(message.TargetID).MsgType(larkim.MsgTypeInteractive).Content(payload).Build()).Build()
	resp, err = c.client.Im.V1.Message.Create(ctx, req)
	if err != nil {
		return domain.ReceiptEntity{}, fmt.Errorf("feishu: create message: %w", err)
	}
	if !resp.Success() {
		return domain.ReceiptEntity{}, fmt.Errorf("feishu: create message failed: code=%d message=%s", resp.Code, resp.Msg)
	}
	id := ""
	if resp.Data != nil && resp.Data.MessageId != nil {
		id = *resp.Data.MessageId
	}
	return domain.ReceiptEntity{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
}
