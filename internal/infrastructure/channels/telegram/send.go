package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
}

func (c *Channel) Send(ctx context.Context, msg domain.Outbound) (domain.ReceiptEntity, error) {
	params := map[string]any{"chat_id": msg.TargetID, "text": msg.Content, "parse_mode": "HTML"}
	if msg.ReplyToMessageID != "" {
		if id, err := strconv.Atoi(msg.ReplyToMessageID); err == nil {
			params["reply_parameters"] = map[string]any{"message_id": id}
		}
	}
	var result sentMessage
	if err := c.call(ctx, "sendMessage", params, &result); err != nil {
		return domain.ReceiptEntity{}, err
	}
	return domain.ReceiptEntity{MessageID: strconv.Itoa(result.MessageID), AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) call(ctx context.Context, method string, params map[string]any, result any) error {
	body, _ := json.Marshal(params)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram %s: %w", method, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	var envelope apiResponse[json.RawMessage]
	if err = json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if !envelope.OK {
		return fmt.Errorf("telegram %s failed: %s", method, envelope.Description)
	}
	if result != nil && len(envelope.Result) > 0 {
		return json.Unmarshal(envelope.Result, result)
	}
	return nil
}
