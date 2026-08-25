package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Send(ctx context.Context, msg domain.Outbound) (domain.ReceiptEntity, error) {
	switch c.config.Kind {
	case "wecom":
		body := map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": msg.Content}}
		if err := c.postJSON(ctx, c.config.WebhookURL, body); err != nil {
			return domain.ReceiptEntity{}, err
		}
	case "wecom_app":
		token, err := c.getToken(ctx)
		if err != nil {
			return domain.ReceiptEntity{}, err
		}
		body := map[string]any{"touser": msg.TargetID, "msgtype": "markdown", "agentid": c.config.AgentID, "markdown": map[string]string{"content": msg.Content}}
		endpoint := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
		if err := c.postJSON(ctx, endpoint, body); err != nil {
			return domain.ReceiptEntity{}, err
		}
	case "wecom_aibot":
		raw, ok := c.responseURLs.Load(msg.TargetID)
		if !ok {
			return domain.ReceiptEntity{}, errors.New("wecom_aibot: response url is unavailable")
		}
		endpoint, _ := raw.(string)
		if err := c.postJSON(ctx, endpoint, map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": msg.Content}}); err != nil {
			return domain.ReceiptEntity{}, err
		}
	}
	return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) postJSON(ctx context.Context, endpoint string, value any) error {
	body, _ := json.Marshal(value)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = c.client.Do(ctx, "wecom", "post", req, func(_ context.Context, response *http.Response) (any, error) {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil, nil
	})
	return err
}
