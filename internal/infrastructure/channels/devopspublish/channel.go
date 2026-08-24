package devopspublish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID            string
	Enabled       bool
	ServerURL     string
	AccountID     string
	Credential    string
	AllowFrom     []string
	ReconnectMin  time.Duration
	ReconnectMax  time.Duration
	AckTimeout    time.Duration
	MediaStore    channelruntime.MediaStore
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}

type Channel struct {
	config  Config
	running atomic.Bool
	cancel  context.CancelFunc
	sink    channelruntime.Sink
	mu      sync.RWMutex
	conn    *connection
	seen    sync.Map
}
type connection struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
	ackMu   sync.Mutex
	acks    map[string]chan frame
	closed  chan struct{}
	once    sync.Once
}
type frame struct {
	Type      string          `json:"type"`
	EventID   string          `json:"event_id,omitempty"`
	Status    string          `json:"status,omitempty"`
	AccountID string          `json:"account_id,omitempty"`
	Error     string          `json:"error,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}
type event struct {
	SchemaVersion    string `json:"schema_version,omitempty"`
	EventID          string `json:"event_id"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	Type             string `json:"type,omitempty"`
	ConversationID   string `json:"conversation_id"`
	MessageID        string `json:"message_id"`
	ReplyToMessageID string `json:"reply_to_message_id,omitempty"`
	Sender           struct {
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
	} `json:"sender"`
	Content     string                     `json:"content"`
	MessageType string                     `json:"message_type,omitempty"`
	Status      string                     `json:"status,omitempty"`
	Attachments []channelruntime.MediaPart `json:"attachments,omitempty"`
	Metadata    map[string]string          `json:"metadata,omitempty"`
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.ServerURL) == "" || strings.TrimSpace(cfg.AccountID) == "" || strings.TrimSpace(cfg.Credential) == "" {
		return nil, errors.New("devops_publish: id, server_url, account_id and credential are required")
	}
	if cfg.ReconnectMin <= 0 {
		cfg.ReconnectMin = time.Second
	}
	if cfg.ReconnectMax < cfg.ReconnectMin {
		cfg.ReconnectMax = 30 * time.Second
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 10 * time.Second
	}
	return &Channel{config: cfg}, nil
}
func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "devops_publish", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "media", "ack", "reconnect", "edit", "placeholder", "streaming"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }
func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("devops_publish: sink is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.sink = sink
	c.mu.Unlock()
	go c.supervise(runCtx)
	return nil
}
func (c *Channel) Stop(context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		conn.close()
	}
	c.running.Store(false)
	return nil
}
func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return channelruntime.ErrChannelDisabled
	}
	if !c.Running() {
		return errors.New("devops_publish: connection is not running")
	}
	return ctx.Err()
}
func (c *Channel) supervise(ctx context.Context) {
	backoff := c.config.ReconnectMin
	for ctx.Err() == nil {
		conn, err := c.connect(ctx)
		if err != nil {
			if !wait(ctx, backoff) {
				return
			}
			backoff *= 2
			if backoff > c.config.ReconnectMax {
				backoff = c.config.ReconnectMax
			}
			continue
		}
		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()
		c.running.Store(true)
		backoff = c.config.ReconnectMin
		err = c.read(ctx, conn)
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
		conn.close()
		c.running.Store(false)
		if ctx.Err() != nil {
			return
		}
		if !wait(ctx, backoff) {
			return
		}
	}
}
func (c *Channel) connect(ctx context.Context) (*connection, error) {
	u, err := url.Parse(c.config.ServerURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, errors.New("devops_publish: server_url must use ws or wss")
	}
	header := http.Header{"Authorization": []string{"Bearer " + c.config.Credential}, "X-OpsClaw-Account-Id": []string{c.config.AccountID}}
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"opsclaw.devops-publish.v1"}
	conn, resp, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("devops_publish: handshake %s: %w", resp.Status, err)
		}
		return nil, err
	}
	result := &connection{conn: conn, acks: make(map[string]chan frame), closed: make(chan struct{})}
	if err := conn.SetReadDeadline(time.Now().Add(c.config.AckTimeout)); err != nil {
		result.close()
		return nil, err
	}
	var ready frame
	if err := conn.ReadJSON(&ready); err != nil {
		result.close()
		return nil, err
	}
	if ready.Type != "connection.ready" || (ready.AccountID != "" && ready.AccountID != c.config.AccountID) {
		result.close()
		return nil, errors.New("devops_publish: invalid connection.ready")
	}
	_ = conn.SetReadDeadline(time.Time{})
	return result, nil
}
func (c *Channel) read(ctx context.Context, conn *connection) error {
	for {
		var raw json.RawMessage
		if err := conn.conn.ReadJSON(&raw); err != nil {
			return err
		}
		var f frame
		if err := json.Unmarshal(raw, &f); err != nil {
			return err
		}
		if f.Type == "event.ack" {
			conn.deliver(f)
			continue
		}
		var e event
		if err := json.Unmarshal(raw, &e); err != nil {
			continue
		}
		if e.EventID == "" {
			e.EventID = f.EventID
		}
		if e.EventID != "" {
			if _, loaded := c.seen.LoadOrStore(e.EventID, time.Now()); loaded {
				continue
			}
		}
		if !allowed(c.config.AllowFrom, e.Sender.ID) {
			continue
		}
		c.mu.RLock()
		sink := c.sink
		c.mu.RUnlock()
		if sink != nil {
			sink(channelruntime.Inbound{ChannelID: c.config.ID, MessageID: e.MessageID, ChatID: e.ConversationID, SenderID: e.Sender.ID, SenderName: e.Sender.DisplayName, Content: e.Content, Media: e.Attachments, Metadata: e.Metadata, ReceivedAt: time.Now().UTC()})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}
func (c *Channel) send(ctx context.Context, e event) (string, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return "", errors.New("devops_publish: connection is not running")
	}
	if e.EventID == "" {
		e.EventID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	waiter := conn.register(e.EventID)
	body, _ := json.Marshal(e)
	if err := conn.write(body); err != nil {
		conn.remove(e.EventID)
		return "", err
	}
	timer := time.NewTimer(c.config.AckTimeout)
	defer timer.Stop()
	select {
	case ack := <-waiter:
		conn.remove(e.EventID)
		if ack.Status != "accepted" && ack.Status != "duplicate" {
			return "", fmt.Errorf("devops_publish: event rejected: %s", ack.Error)
		}
		return e.MessageID, nil
	case <-timer.C:
		conn.remove(e.EventID)
		return "", errors.New("devops_publish: event acknowledgement timeout")
	case <-ctx.Done():
		conn.remove(e.EventID)
		return "", ctx.Err()
	}
}
func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	id := msg.ID
	if id == "" {
		id = fmt.Sprintf("message-%d", time.Now().UnixNano())
	}
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: id, IdempotencyKey: id, Type: "message.created", Status: "running", MessageType: "text", MessageID: id, ConversationID: msg.TargetID, ReplyToMessageID: msg.ReplyToMessageID, Content: msg.Content, Metadata: msg.Metadata})
	if err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) SendMedia(ctx context.Context, msg channelruntime.OutboundMedia) (channelruntime.Receipt, error) {
	id := msg.ID
	if id == "" {
		id = fmt.Sprintf("media-%d", time.Now().UnixNano())
	}
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: id, IdempotencyKey: id, Type: "message.created", Status: "running", MessageType: "file", MessageID: id, ConversationID: msg.TargetID, Attachments: msg.Parts, Metadata: msg.Metadata})
	if err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) EditMessage(ctx context.Context, targetID, messageID, content string) error {
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: fmt.Sprintf("edit-%d", time.Now().UnixNano()), IdempotencyKey: messageID, Type: "message.updated", Status: "running", MessageID: messageID, ConversationID: targetID, Content: content})
	return err
}
func (c *Channel) EditMessageWithState(ctx context.Context, targetID, messageID, content, state string, metadata map[string]string) error {
	typ, status := "placeholder.updated", "running"
	if state == "final" {
		typ, status = "message.final", "completed"
	}
	if state == "failed" {
		typ, status = "message.failed", "failed"
	}
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: fmt.Sprintf("edit-%d", time.Now().UnixNano()), IdempotencyKey: messageID, Type: typ, Status: status, MessageID: messageID, ConversationID: targetID, Content: content, Metadata: metadata})
	return err
}
func (c *Channel) SendPlaceholder(ctx context.Context, targetID, replyTo, content string) (string, error) {
	id := fmt.Sprintf("placeholder-%d", time.Now().UnixNano())
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: id, IdempotencyKey: id, Type: "placeholder.created", Status: "accepted", MessageType: "text", MessageID: id, ConversationID: targetID, ReplyToMessageID: replyTo, Content: content})
	return id, err
}
func (c *Channel) StartStreamingMessage(ctx context.Context, targetID, replyTo, content string) (string, error) {
	id := fmt.Sprintf("stream-%d", time.Now().UnixNano())
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: id, IdempotencyKey: id, Type: "message.created", Status: "running", MessageType: "text", MessageID: id, ConversationID: targetID, ReplyToMessageID: replyTo, Content: content})
	return id, err
}
func (c *Channel) UpdateStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: fmt.Sprintf("stream-%d", time.Now().UnixNano()), IdempotencyKey: messageID, Type: "placeholder.updated", Status: "running", MessageID: messageID, ConversationID: targetID, Content: content})
	return err
}
func (c *Channel) FinishStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	_, err := c.send(ctx, event{SchemaVersion: "1", EventID: fmt.Sprintf("stream-%d", time.Now().UnixNano()), IdempotencyKey: messageID, Type: "message.final", Status: "completed", MessageID: messageID, ConversationID: targetID, Content: content})
	return err
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
func allowed(list []string, id string) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if strings.TrimPrefix(strings.TrimSpace(v), "@") == id {
			return true
		}
	}
	return false
}
func (c *connection) register(id string) <-chan frame {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	ch := make(chan frame, 1)
	c.acks[id] = ch
	return ch
}
func (c *connection) remove(id string) { c.ackMu.Lock(); delete(c.acks, id); c.ackMu.Unlock() }
func (c *connection) deliver(f frame) {
	c.ackMu.Lock()
	ch := c.acks[f.EventID]
	c.ackMu.Unlock()
	if ch != nil {
		select {
		case ch <- f:
		default:
		}
	}
}
func (c *connection) write(body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, body)
}
func (c *connection) close() { c.once.Do(func() { close(c.closed); _ = c.conn.Close() }) }

var _ channelruntime.Channel = (*Channel)(nil)
var _ channelruntime.MediaChannel = (*Channel)(nil)
