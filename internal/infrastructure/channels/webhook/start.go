package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
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
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.sink = sink
	c.cancel = cancel
	c.mu.Unlock()
	if c.config.Listen != "" {
		mux := http.NewServeMux()
		mux.HandleFunc(c.config.Path, c.serve)
		mux.HandleFunc(c.config.Path+"/health", c.health)
		c.server = &http.Server{Addr: c.config.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 10 * time.Second}
		go func() {
			if err := c.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				c.running.Store(false)
			}
		}()
	}
	c.running.Store(true)
	_ = runCtx
	return nil
}

func (c *Channel) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if c.config.Secret != "" {
		signature := strings.TrimSpace(r.Header.Get("X-Channel-Signature"))
		mac := hmac.New(sha256.New, []byte(c.config.Secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(expected)) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}
	var msg envelope
	if err = json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	if msg.MessageID != "" {
		if _, loaded := c.seen.LoadOrStore(msg.MessageID, time.Now()); loaded {
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}
	if !allowed(c.config.AllowFrom, msg.SenderID) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(domain.Inbound{ChannelID: c.config.ID, MessageID: msg.MessageID, ChatID: msg.ChatID, SenderID: msg.SenderID, SenderName: msg.SenderName, Content: msg.Content, Media: msg.Media, Metadata: msg.Metadata, ReceivedAt: time.Now().UTC()})
	}
	w.WriteHeader(http.StatusAccepted)
}
func (c *Channel) health(w http.ResponseWriter, _ *http.Request) {
	if !c.Running() {
		http.Error(w, "not running", 503)
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
