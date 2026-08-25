package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func (c *Channel) SendMedia(ctx context.Context, message domain.OutboundMedia) (domain.ReceiptEntity, error) {
	if !c.Running() {
		return domain.ReceiptEntity{}, errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(message.TargetID) == "" || len(message.Parts) == 0 {
		return domain.ReceiptEntity{}, errors.New("feishu: media target and parts are required")
	}
	var messageID string
	for _, part := range message.Parts {
		if c.config.MediaStore == nil {
			return domain.ReceiptEntity{}, errors.New("feishu: media store is not configured")
		}
		resource, err := c.config.MediaStore.Open(ctx, part.Ref)
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		var sentID string
		switch part.Type {
		case "image":
			sentID, err = c.sendImage(ctx, message.TargetID, resource.Reader)
		default:
			sentID, err = c.sendFile(ctx, message.TargetID, resource.Reader, part.Filename, part.Type)
		}
		_ = resource.Reader.Close()
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		if sentID != "" {
			messageID = sentID
		}
	}
	return domain.ReceiptEntity{MessageID: messageID, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) sendImage(ctx context.Context, chatID string, file io.Reader) (string, error) {
	response, err := c.client.Im.V1.Image.Create(ctx, larkim.NewCreateImageReqBuilder().Body(larkim.NewCreateImageReqBodyBuilder().ImageType("message").Image(file).Build()).Build())
	if err != nil {
		return "", fmt.Errorf("feishu: upload image: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.ImageKey == nil {
		return "", fmt.Errorf("feishu: upload image failed: code=%d message=%s", response.Code, response.Msg)
	}
	content, _ := json.Marshal(map[string]string{"image_key": *response.Data.ImageKey})
	created, err := c.client.Im.V1.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(chatID).MsgType(larkim.MsgTypeImage).Content(string(content)).Build()).Build())
	if err != nil || !created.Success() {
		return "", fmt.Errorf("feishu: send image: %w", err)
	}
	if created.Data != nil && created.Data.MessageId != nil {
		return *created.Data.MessageId, nil
	}
	return "", nil
}
func (c *Channel) sendFile(ctx context.Context, chatID string, file io.Reader, filename, kind string) (string, error) {
	fileType := "stream"
	if kind == "audio" {
		fileType = "opus"
	}
	if kind == "video" {
		fileType = "mp4"
	}
	response, err := c.client.Im.V1.File.Create(ctx, larkim.NewCreateFileReqBuilder().Body(larkim.NewCreateFileReqBodyBuilder().FileType(fileType).FileName(filename).File(file).Build()).Build())
	if err != nil {
		return "", fmt.Errorf("feishu: upload file: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.FileKey == nil {
		return "", fmt.Errorf("feishu: upload file failed: code=%d message=%s", response.Code, response.Msg)
	}
	content, _ := json.Marshal(map[string]string{"file_key": *response.Data.FileKey})
	created, err := c.client.Im.V1.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(chatID).MsgType(larkim.MsgTypeFile).Content(string(content)).Build()).Build())
	if err != nil || !created.Success() {
		return "", fmt.Errorf("feishu: send file: %w", err)
	}
	if created.Data != nil && created.Data.MessageId != nil {
		return *created.Data.MessageId, nil
	}
	return "", nil
}
