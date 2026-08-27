package wecom

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	stdhttp "net/http"
	"net/url"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/transport/http"
)

type mediaUploadResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
	MediaID string `json:"media_id"`
}

func (c *Channel) SendMedia(ctx context.Context, msg domain.OutboundMedia) (domain.ReceiptEntity, error) {
	if c.config.MediaStore == nil {
		return domain.ReceiptEntity{}, errors.New("wecom: media store is not configured")
	}
	if c.config.Kind == "wecom_app" {
		token, err := c.getToken(ctx)
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		for _, part := range msg.Parts {
			resource, openErr := c.config.MediaStore.Open(ctx, part.Ref)
			if openErr != nil {
				return domain.ReceiptEntity{}, openErr
			}
			mediaID, uploadErr := c.uploadMedia(ctx, token, part, resource)
			_ = resource.Reader.Close()
			if uploadErr != nil {
				return domain.ReceiptEntity{}, uploadErr
			}
			msgType := "file"
			if part.Type == "image" {
				msgType = "image"
			}
			payload := map[string]any{"touser": msg.TargetID, "msgtype": msgType, "agentid": c.config.AgentID}
			if msgType == "image" {
				payload["image"] = map[string]string{"media_id": mediaID}
			} else {
				payload["file"] = map[string]string{"media_id": mediaID}
			}
			endpoint := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
			if err := c.postJSON(ctx, endpoint, payload); err != nil {
				return domain.ReceiptEntity{}, err
			}
		}
		return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
	}
	if c.config.Kind == "wecom" {
		parts := make([]string, 0, len(msg.Parts))
		for _, part := range msg.Parts {
			if part.Type == "image" {
				resource, err := c.config.MediaStore.Open(ctx, part.Ref)
				if err != nil {
					return domain.ReceiptEntity{}, err
				}
				data, readErr := io.ReadAll(io.LimitReader(resource.Reader, 10<<20))
				_ = resource.Reader.Close()
				if readErr != nil {
					return domain.ReceiptEntity{}, readErr
				}
				hash := md5.Sum(data)
				if err := c.postJSON(ctx, c.config.WebhookURL, map[string]any{"msgtype": "image", "image": map[string]string{"base64": base64.StdEncoding.EncodeToString(data), "md5": fmt.Sprintf("%x", hash)}}); err != nil {
					return domain.ReceiptEntity{}, err
				}
				continue
			}
			parts = append(parts, fmt.Sprintf("[%s] %s", part.Type, part.Ref))
		}
		if len(parts) == 0 {
			return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
		}
		return c.Send(ctx, domain.Outbound{ID: msg.ID, ChannelID: msg.ChannelID, TargetID: msg.TargetID, Content: strings.Join(parts, "\n")})
	}
	parts := make([]string, 0, len(msg.Parts))
	for _, part := range msg.Parts {
		parts = append(parts, fmt.Sprintf("[%s] %s", part.Type, part.Ref))
	}
	return c.Send(ctx, domain.Outbound{ID: msg.ID, ChannelID: msg.ChannelID, TargetID: msg.TargetID, Content: strings.Join(parts, "\n")})
}

func (c *Channel) uploadMedia(ctx context.Context, token string, part domain.MediaPartEntity, resource domain.MediaEntity) (string, error) {
	mediaType := "file"
	if part.Type == "image" {
		mediaType = "image"
	}
	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token=" + url.QueryEscape(token) + "&type=" + mediaType
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("media", part.Filename)
	if err != nil {
		return "", err
	}
	if _, err = io.Copy(file, resource.Reader); err != nil {
		return "", err
	}
	if err = writer.Close(); err != nil {
		return "", err
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, endpoint, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	result, err := http.Do[mediaUploadResponse](ctx, c.client, "wecom", "uploadMedia", req, func(_ context.Context, response *stdhttp.Response) (mediaUploadResponse, error) {
		var result mediaUploadResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			return mediaUploadResponse{}, err
		}
		return result, nil
	})
	if err != nil {
		return "", err
	}
	if result.ErrCode != 0 || result.MediaID == "" {
		return "", fmt.Errorf("wecom: media upload failed: %s", result.ErrMsg)
	}
	return result.MediaID, nil
}
