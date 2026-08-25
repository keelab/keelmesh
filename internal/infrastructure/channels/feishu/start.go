package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if strings.TrimSpace(c.config.AppID) == "" || strings.TrimSpace(c.config.AppSecret) == "" {
		return errors.New("feishu: app_id and app_secret are required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	dispatcher := larkdispatcher.NewEventDispatcher(c.config.VerificationToken, c.config.EncryptKey).OnP2MessageReceiveV1(c.receive)
	ws := larkws.NewClient(c.config.AppID, c.config.AppSecret, larkws.WithEventHandler(dispatcher))
	c.mu.Lock()
	c.sink = sink
	c.cancel = cancel
	c.ws = ws
	c.mu.Unlock()
	c.running.Store(true)
	go func() {
		if err := ws.Start(runCtx); err != nil && runCtx.Err() == nil {
			c.running.Store(false)
		}
	}()
	return nil
}
func (c *Channel) receive(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	if event == nil || event.Event == nil || event.Event.Message == nil {
		return nil
	}
	message := event.Event.Message
	messageID := value(message.MessageId)
	if messageID != "" {
		if _, loaded := c.seen.LoadOrStore(messageID, time.Now()); loaded {
			return nil
		}
	}
	chatID := value(message.ChatId)
	if chatID == "" {
		return nil
	}
	senderID := ""
	if event.Event.Sender != nil && event.Event.Sender.SenderId != nil {
		senderID = value(event.Event.Sender.SenderId.OpenId)
		if senderID == "" {
			senderID = value(event.Event.Sender.SenderId.UserId)
		}
	}
	if len(c.config.AllowFrom) > 0 && !contains(c.config.AllowFrom, senderID) {
		return nil
	}
	content := value(message.Content)
	media := make([]domain.MediaPartEntity, 0, 1)
	if value(message.MessageType) == larkim.MsgTypeText {
		var text struct {
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(content), &text) == nil {
			content = text.Text
		}
	} else {
		var payload map[string]any
		if json.Unmarshal([]byte(content), &payload) == nil {
			key := ""
			for _, field := range []string{"image_key", "file_key", "audio_key", "video_key"} {
				if value, ok := payload[field].(string); ok {
					key = value
					break
				}
			}
			if key != "" {
				kind := value(message.MessageType)
				if kind == "file" || kind == "audio" || kind == "video" {
					if part, err := c.ingestInboundFile(ctx, kind, key, payload); err == nil {
						media = append(media, part)
					}
				} else if kind == "image" {
					if part, err := c.ingestInboundImage(ctx, key); err == nil {
						media = append(media, part)
					}
				}
			}
			content = ""
		}
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(domain.Inbound{ChannelID: c.config.ID, MessageID: messageID, ChatID: chatID, SenderID: senderID, Content: content, Media: media, Metadata: map[string]string{"message_type": value(message.MessageType)}, ReceivedAt: time.Now().UTC()})
	}
	return nil
}
func (c *Channel) ingestInboundImage(ctx context.Context, key string) (domain.MediaPartEntity, error) {
	if c.config.MediaStore == nil {
		return domain.MediaPartEntity{}, errors.New("feishu: media store is not configured")
	}
	response, err := c.client.Im.V1.Image.Get(ctx, larkim.NewGetImageReqBuilder().ImageKey(key).Build())
	if err != nil || !response.Success() {
		if err != nil {
			return domain.MediaPartEntity{}, err
		}
		return domain.MediaPartEntity{}, fmt.Errorf("feishu: download image failed: code=%d message=%s", response.Code, response.Msg)
	}
	part, err := c.config.MediaStore.Store(ctx, key+".png", "image/png", response.File)
	part.Type = "image"
	return part, err
}

func (c *Channel) ingestInboundFile(ctx context.Context, kind, key string, payload map[string]any) (domain.MediaPartEntity, error) {
	if c.config.MediaStore == nil {
		return domain.MediaPartEntity{}, errors.New("feishu: media store is not configured")
	}
	response, err := c.client.Im.V1.File.Get(ctx, larkim.NewGetFileReqBuilder().FileKey(key).Build())
	if err != nil || !response.Success() {
		if err != nil {
			return domain.MediaPartEntity{}, err
		}
		return domain.MediaPartEntity{}, fmt.Errorf("feishu: download file failed: code=%d message=%s", response.Code, response.Msg)
	}
	filename, _ := payload["file_name"].(string)
	if filename == "" {
		filename = response.FileName
	}
	part, err := c.config.MediaStore.Store(ctx, filename, "application/octet-stream", response.File)
	part.Type = kind
	part.Filename = filename
	return part, err
}

func value(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
