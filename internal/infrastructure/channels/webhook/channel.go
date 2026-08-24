package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID            string
	Enabled       bool
	OutboundURL   string
	Listen        string
	Path          string
	Secret        string
	AllowFrom     []string
	MediaStore    channelruntime.MediaStore
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}

type Channel struct {
	config  Config
	client  *http.Client
	server  *http.Server
	cancel  context.CancelFunc
	sink    channelruntime.Sink
	running atomic.Bool
	mu      sync.Mutex
	seen    sync.Map
}

type envelope struct {
	MessageID  string                     `json:"message_id"`
	ChatID     string                     `json:"chat_id"`
	SenderID   string                     `json:"sender_id"`
	SenderName string                     `json:"sender_name"`
	Content    string                     `json:"content"`
	Media      []channelruntime.MediaPart `json:"media"`
	Metadata   map[string]string          `json:"metadata"`
	Timestamp  string                     `json:"timestamp"`
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("webhook: id is required")
	}
	if cfg.Path == "" {
		cfg.Path = "/webhook/" + cfg.ID
	}
	return &Channel{config: cfg, client: &http.Client{Timeout: 15 * time.Second}}, nil
}
func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "webhook", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "media", "signed_webhook"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }
func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
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
func (c *Channel) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	server := c.server
	c.server = nil
	c.mu.Unlock()
	c.running.Store(false)
	if server != nil {
		return server.Shutdown(ctx)
	}
	return nil
}
func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return channelruntime.ErrChannelDisabled
	}
	if c.config.OutboundURL == "" {
		if c.Running() {
			return nil
		}
		return errors.New("webhook: listener is not running")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.config.OutboundURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("webhook: probe returned %s", resp.Status)
	}
	return nil
}
func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	if strings.TrimSpace(c.config.OutboundURL) == "" {
		return channelruntime.Receipt{}, errors.New("webhook: outbound_url is not configured")
	}
	body, _ := json.Marshal(envelope{MessageID: msg.ID, ChatID: msg.TargetID, Content: msg.Content, Metadata: msg.Metadata})
	if err := c.post(ctx, c.config.OutboundURL, body); err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) SendMedia(ctx context.Context, msg channelruntime.OutboundMedia) (channelruntime.Receipt, error) {
	if strings.TrimSpace(c.config.OutboundURL) == "" {
		return channelruntime.Receipt{}, errors.New("webhook: outbound_url is not configured")
	}
	body, _ := json.Marshal(struct {
		Envelope envelope                   `json:"message"`
		Parts    []channelruntime.MediaPart `json:"media"`
	}{Envelope: envelope{MessageID: msg.ID, ChatID: msg.TargetID, Metadata: msg.Metadata}, Parts: msg.Parts})
	if err := c.post(ctx, c.config.OutboundURL, body); err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
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
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook: outbound returned %s", resp.Status)
	}
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
		sink(channelruntime.Inbound{ChannelID: c.config.ID, MessageID: msg.MessageID, ChatID: msg.ChatID, SenderID: msg.SenderID, SenderName: msg.SenderName, Content: msg.Content, Media: msg.Media, Metadata: msg.Metadata, ReceivedAt: time.Now().UTC()})
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

var _ channelruntime.Channel = (*Channel)(nil)
