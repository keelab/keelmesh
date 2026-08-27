package qq

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/token"
)

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
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
	return c.publish(data.ID, data.Author.ID, data.GroupID, data.Content, map[string]string{"scope": "group", "group_id": data.GroupID, "mentioned": "true"})
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
		sink(domain.Inbound{ChannelID: c.config.ID, MessageID: id, ChatID: chat, SenderID: sender, Content: strings.TrimSpace(content), Metadata: metadata, ReceivedAt: time.Now().UTC()})
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
