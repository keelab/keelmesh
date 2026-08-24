package dingtalk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID            string
	Enabled       bool
	ClientID      string
	ClientSecret  string
	AllowFrom     []string
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config   Config
	stream   *client.StreamClient
	cancel   context.CancelFunc
	sink     channelruntime.Sink
	running  atomic.Bool
	webhooks sync.Map
	mu       sync.Mutex
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("dingtalk: id, client_id and client_secret are required")
	}
	return &Channel{config: cfg}, nil
}
func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "dingtalk", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "groups", "session_reply"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }
func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("dingtalk: sink is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.sink = sink
	c.mu.Unlock()
	c.stream = client.NewStreamClient(client.WithAppCredential(client.NewAppCredentialConfig(c.config.ClientID, c.config.ClientSecret)), client.WithAutoReconnect(true))
	c.stream.RegisterChatBotCallbackRouter(c.receive)
	if err := c.stream.Start(runCtx); err != nil {
		return err
	}
	c.running.Store(true)
	return nil
}
func (c *Channel) Stop(context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	stream := c.stream
	c.stream = nil
	c.mu.Unlock()
	if stream != nil {
		stream.Close()
	}
	c.running.Store(false)
	return nil
}
func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return channelruntime.ErrChannelDisabled
	}
	if !c.Running() {
		return errors.New("dingtalk: stream is not running")
	}
	return ctx.Err()
}
func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	raw, ok := c.webhooks.Load(msg.TargetID)
	if !ok {
		return channelruntime.Receipt{}, fmt.Errorf("dingtalk: no session webhook for %q", msg.TargetID)
	}
	hook, ok := raw.(string)
	if !ok || hook == "" {
		return channelruntime.Receipt{}, errors.New("dingtalk: invalid session webhook")
	}
	replier := chatbot.NewChatbotReplier()
	if err := replier.SimpleReplyMarkdown(ctx, hook, []byte("channelcore"), []byte(msg.Content)); err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) receive(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	content := data.Text.Content
	if content == "" {
		if m, ok := data.Content.(map[string]any); ok {
			if v, ok := m["content"].(string); ok {
				content = v
			}
		}
	}
	if strings.TrimSpace(content) == "" {
		return nil, nil
	}
	sender := data.SenderStaffId
	chatID := sender
	if data.ConversationType != "1" {
		chatID = data.ConversationId
	}
	if !allowed(c.config.AllowFrom, sender) {
		return nil, nil
	}
	c.webhooks.Store(chatID, data.SessionWebhook)
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(channelruntime.Inbound{ChannelID: c.config.ID, ChatID: chatID, SenderID: sender, SenderName: data.SenderNick, Content: strings.TrimSpace(content), Metadata: map[string]string{"conversation_id": data.ConversationId, "conversation_type": data.ConversationType, "session_webhook": data.SessionWebhook}, ReceivedAt: time.Now().UTC()})
	}
	return nil, nil
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

var _ channelruntime.Channel = (*Channel)(nil)
