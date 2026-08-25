package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

type fileInfo struct {
	FilePath string `json:"file_path"`
}

func (c *Channel) SendMedia(ctx context.Context, msg domain.OutboundMedia) (domain.ReceiptEntity, error) {
	if c.config.MediaStore == nil {
		return domain.ReceiptEntity{}, errors.New("telegram: media store is not configured")
	}
	var last string
	for _, part := range msg.Parts {
		if strings.HasPrefix(part.Ref, "telegram-file://") {
			return domain.ReceiptEntity{}, errors.New("telegram: inbound file refs are not outbound media refs")
		}
		resource, err := c.config.MediaStore.Open(ctx, part.Ref)
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		field := "document"
		endpoint := "sendDocument"
		if part.Type == "image" {
			field = "photo"
			endpoint = "sendPhoto"
		}
		if part.Type == "video" {
			field = "video"
			endpoint = "sendVideo"
		}
		if part.Type == "audio" {
			field = "audio"
			endpoint = "sendAudio"
		}
		id, err := c.upload(ctx, endpoint, field, msg.TargetID, part.Filename, part.Caption, resource.Reader)
		_ = resource.Reader.Close()
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		last = id
	}
	return domain.ReceiptEntity{MessageID: last, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) upload(ctx context.Context, endpoint, field, target, filename, caption string, reader io.Reader) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("chat_id", target)
	if caption != "" {
		_ = mw.WriteField("caption", caption)
	}
	part, err := mw.CreateFormFile(field, filename)
	if err != nil {
		return "", err
	}
	if _, err = io.Copy(part, reader); err != nil {
		return "", err
	}
	if err = mw.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+endpoint, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	value, err := c.client.Do(ctx, "telegram", endpoint, req, func(_ context.Context, resp *http.Response) (any, error) {
		var envelope apiResponse[sentMessage]
		if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
			return "", err
		}
		if !envelope.OK {
			return "", fmt.Errorf("telegram %s failed: %s", endpoint, envelope.Description)
		}
		return strconv.Itoa(envelope.Result.MessageID), nil
	})
	if err != nil {
		return "", err
	}
	messageID, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("telegram %s: unexpected response type %T", endpoint, value)
	}
	return messageID, nil
}
