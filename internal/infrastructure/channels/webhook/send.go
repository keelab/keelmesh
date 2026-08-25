package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Send(ctx context.Context, msg domain.Outbound) (domain.ReceiptEntity, error) {
	if strings.TrimSpace(c.config.OutboundURL) == "" {
		return domain.ReceiptEntity{}, errors.New("webhook: outbound_url is not configured")
	}
	body, _ := json.Marshal(envelope{MessageID: msg.ID, ChatID: msg.TargetID, Content: msg.Content, Metadata: msg.Metadata})
	if err := c.post(ctx, c.config.OutboundURL, body); err != nil {
		return domain.ReceiptEntity{}, err
	}
	return domain.ReceiptEntity{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) post(ctx context.Context, target string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.config.Secret != "" {
		mac := hmac.New(sha256.New, []byte(c.config.Secret))
		_, _ = mac.Write(body)
		req.Header.Set("X-Channel-Signature", hex.EncodeToString(mac.Sum(nil)))
	}
	_, err = c.client.Do(ctx, "webhook", "post", req, func(_ context.Context, resp *http.Response) (any, error) {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil
	})
	return err
}
