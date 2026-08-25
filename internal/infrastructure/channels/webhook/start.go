package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	stdhttp "net/http"
	"strings"
	"time"

	"errors"

	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("webhook: sink is required")
	}
	_, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.sink = sink
	c.cancel = cancel
	c.mu.Unlock()
	c.running.Store(true)
	return nil
}

func (c *Channel) serve(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if r.Method != stdhttp.MethodPost {
		stdhttp.Error(w, "method not allowed", stdhttp.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		stdhttp.Error(w, "invalid body", 400)
		return
	}
	if c.config.Secret != "" {
		signature := strings.TrimSpace(r.Header.Get("X-Channel-Signature"))
		mac := hmac.New(sha256.New, []byte(c.config.Secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(expected)) {
			stdhttp.Error(w, "invalid signature", stdhttp.StatusUnauthorized)
			return
		}
	}
	var msg envelope
	if err = json.Unmarshal(body, &msg); err != nil {
		stdhttp.Error(w, "invalid json", 400)
		return
	}
	if msg.MessageID != "" {
		if _, loaded := c.seen.LoadOrStore(msg.MessageID, time.Now()); loaded {
			w.WriteHeader(stdhttp.StatusAccepted)
			return
		}
	}
	if !allowed(c.config.AllowFrom, msg.SenderID) {
		w.WriteHeader(stdhttp.StatusAccepted)
		return
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(domain.Inbound{ChannelID: c.config.ID, MessageID: msg.MessageID, ChatID: msg.ChatID, SenderID: msg.SenderID, SenderName: msg.SenderName, Content: msg.Content, Media: msg.Media, Metadata: msg.Metadata, ReceivedAt: time.Now().UTC()})
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}
func (c *Channel) health(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
	if !c.Running() {
		stdhttp.Error(w, "not running", 503)
		return
	}
	w.WriteHeader(200)
}

func allowed(list []string, id string) bool {
	if len(list) == 0 {
		return true
	}
	for _, item := range list {
		if strings.TrimPrefix(strings.TrimSpace(item), "@") == id {
			return true
		}
	}
	return false
}
