package qq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/openapi"
	"github.com/tencent-connect/botgo/token"
	"golang.org/x/oauth2"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID            string
	Enabled       bool
	AppID         string
	AppSecret     string
	AllowFrom     []string
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config      Config
	api         openapi.OpenAPI
	tokenSource oauth2.TokenSource
	session     botgo.SessionManager
	cancel      context.CancelFunc
	sink        channelruntime.Sink
	running     atomic.Bool
	mu          sync.Mutex
	seen        sync.Map
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.AppID) == "" || strings.TrimSpace(cfg.AppSecret) == "" {
		return nil, errors.New("qq: id, app_id and app_secret are required")
	}
	return &Channel{config: cfg}, nil
}
func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "qq", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "groups"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }
func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("qq: sink is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.sink = sink
	c.mu.Unlock()
	c.tokenSource = token.NewQQBotTokenSource(&token.QQBotCredentials{AppID: c.config.AppID, AppSecret: c.config.AppSecret})
	if err := token.StartRefreshAccessToken(runCtx, c.tokenSource); err != nil {
		return err
	}
	c.api = botgo.NewOpenAPI(c.config.AppID, c.tokenSource).WithTimeout(10 * time.Second)
	intent := event.RegisterHandlers(c.handleC2C, c.handleGroup)
	ws, err := c.api.WS(runCtx, nil, "")
	if err != nil {
		return err
	}
	c.session = botgo.NewSessionManager()
	go func() {
		if err := c.session.Start(ws, c.tokenSource, &intent); err != nil {
			c.running.Store(false)
		}
	}()
	c.running.Store(true)
	return nil
}
func (c *Channel) Stop(context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.mu.Unlock()
	c.running.Store(false)
	return nil
}
func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return channelruntime.ErrChannelDisabled
	}
	if !c.Running() {
		return errors.New("qq: websocket is not running")
	}
	return ctx.Err()
}
func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	if strings.HasPrefix(msg.TargetID, "group:") {
		_, err := c.api.PostGroupMessage(ctx, strings.TrimPrefix(msg.TargetID, "group:"), &dto.MessageToCreate{Content: msg.Content})
		if err != nil {
			return channelruntime.Receipt{}, err
		}
	} else {
		_, err := c.api.PostC2CMessage(ctx, strings.TrimPrefix(msg.TargetID, "user:"), &dto.MessageToCreate{Content: msg.Content})
		if err != nil {
			return channelruntime.Receipt{}, err
		}
	}
	return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) handleC2C(_ *dto.WSPayload, data *dto.WSC2CMessageData) error {
	if data == nil || data.Author == nil {
		return nil
	}
	return c.publish(data.ID, data.Author.ID, data.Author.ID, data.Content, map[string]string{"scope": "direct"})
}
func (c *Channel) handleGroup(_ *dto.WSPayload, data *dto.WSGroupATMessageData) error {
	if data == nil || data.Author == nil {
		return nil
	}
	return c.publish(data.ID, data.Author.ID, data.GroupID, data.Content, map[string]string{"scope": "group", "group_id": data.GroupID})
}
func (c *Channel) publish(id, sender, chat, content string, metadata map[string]string) error {
	if id != "" {
		if _, loaded := c.seen.LoadOrStore(id, time.Now()); loaded {
			return nil
		}
	}
	if !allowed(c.config.AllowFrom, sender) {
		return nil
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(channelruntime.Inbound{ChannelID: c.config.ID, MessageID: id, ChatID: chat, SenderID: sender, Content: strings.TrimSpace(content), Metadata: metadata, ReceivedAt: time.Now().UTC()})
	}
	return nil
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
