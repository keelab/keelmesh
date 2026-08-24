package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type Config struct {
	ID                string
	Enabled           bool
	AppID             string
	AppSecret         string
	EncryptKey        string
	VerificationToken string
	AllowFrom         []string
	MediaRoot         string
	RatePerSecond     float64
	Burst             int
	QueueSize         int
	MaxRetries        int
	MediaStore        channelruntime.MediaStore
}

type Channel struct {
	config  Config
	client  *lark.Client
	ws      *larkws.Client
	cancel  context.CancelFunc
	sink    channelruntime.Sink
	running atomic.Bool
	mu      sync.Mutex
	seen    sync.Map
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("feishu: channel id is required")
	}
	if cfg.MediaStore == nil && cfg.MediaRoot != "" {
		store, err := channelruntime.NewFileMediaStore(cfg.MediaRoot)
		if err != nil {
			return nil, err
		}
		cfg.MediaStore = store
	}
	return &Channel{config: cfg, client: lark.NewClient(cfg.AppID, cfg.AppSecret)}, nil
}

func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "feishu", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "threads"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}

func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
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

func (c *Channel) Stop(context.Context) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.ws = nil
	c.mu.Unlock()
	c.running.Store(false)
	return nil
}

func (c *Channel) Running() bool { return c.running.Load() }

func (c *Channel) Probe(context.Context) error {
	if !c.config.Enabled {
		return channelruntime.ErrChannelDisabled
	}
	if !c.Running() {
		return errors.New("feishu: websocket is not running")
	}
	return nil
}

func (c *Channel) Send(ctx context.Context, message channelruntime.Outbound) (channelruntime.Receipt, error) {
	if !c.Running() {
		return channelruntime.Receipt{}, errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(message.TargetID) == "" {
		return channelruntime.Receipt{}, errors.New("feishu: target id is required")
	}
	payload, err := json.Marshal(map[string]string{"text": message.Content})
	if err != nil {
		return channelruntime.Receipt{}, err
	}
	var resp *larkim.CreateMessageResp
	if message.ReplyToMessageID != "" {
		replyReq := larkim.NewReplyMessageReqBuilder().MessageId(message.ReplyToMessageID).Body(larkim.NewReplyMessageReqBodyBuilder().MsgType(larkim.MsgTypeText).Content(string(payload)).ReplyInThread(true).Build()).Build()
		replyResp, replyErr := c.client.Im.V1.Message.Reply(ctx, replyReq)
		if replyErr != nil {
			return channelruntime.Receipt{}, fmt.Errorf("feishu: reply message: %w", replyErr)
		}
		if !replyResp.Success() {
			return channelruntime.Receipt{}, fmt.Errorf("feishu: reply message failed: code=%d message=%s", replyResp.Code, replyResp.Msg)
		}
		id := ""
		if replyResp.Data != nil && replyResp.Data.MessageId != nil {
			id = *replyResp.Data.MessageId
		}
		return channelruntime.Receipt{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
	}
	req := larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(message.TargetID).MsgType(larkim.MsgTypeText).Content(string(payload)).Build()).Build()
	resp, err = c.client.Im.V1.Message.Create(ctx, req)
	if err != nil {
		return channelruntime.Receipt{}, fmt.Errorf("feishu: create message: %w", err)
	}
	if !resp.Success() {
		return channelruntime.Receipt{}, fmt.Errorf("feishu: create message failed: code=%d message=%s", resp.Code, resp.Msg)
	}
	id := ""
	if resp.Data != nil && resp.Data.MessageId != nil {
		id = *resp.Data.MessageId
	}
	return channelruntime.Receipt{MessageID: id, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) SendMedia(ctx context.Context, message channelruntime.OutboundMedia) (channelruntime.Receipt, error) {
	if !c.Running() {
		return channelruntime.Receipt{}, errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(message.TargetID) == "" || len(message.Parts) == 0 {
		return channelruntime.Receipt{}, errors.New("feishu: media target and parts are required")
	}
	var messageID string
	for _, part := range message.Parts {
		if c.config.MediaStore == nil {
			return channelruntime.Receipt{}, errors.New("feishu: media store is not configured")
		}
		resource, err := c.config.MediaStore.Open(ctx, part.Ref)
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		var sentID string
		switch part.Type {
		case "image":
			sentID, err = c.sendImage(ctx, message.TargetID, resource.Reader)
		default:
			sentID, err = c.sendFile(ctx, message.TargetID, resource.Reader, part.Filename, part.Type)
		}
		_ = resource.Reader.Close()
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		if sentID != "" {
			messageID = sentID
		}
	}
	return channelruntime.Receipt{MessageID: messageID, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) EditMessage(ctx context.Context, targetID, messageID, content string) error {
	if !c.Running() {
		return errors.New("feishu: channel is not running")
	}
	if strings.TrimSpace(messageID) == "" {
		return errors.New("feishu: message id is required")
	}
	encoded, err := json.Marshal(map[string]string{"text": content})
	if err != nil {
		return err
	}
	response, err := c.client.Im.V1.Message.Patch(ctx, larkim.NewPatchMessageReqBuilder().MessageId(messageID).Body(larkim.NewPatchMessageReqBodyBuilder().Content(string(encoded)).Build()).Build())
	if err != nil {
		return fmt.Errorf("feishu: patch message: %w", err)
	}
	if !response.Success() {
		return fmt.Errorf("feishu: patch message failed: code=%d message=%s", response.Code, response.Msg)
	}
	_ = targetID
	return nil
}

func (c *Channel) EditMessageWithState(ctx context.Context, targetID, messageID, content, _ string, _ map[string]string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}

func (c *Channel) SendPlaceholder(ctx context.Context, targetID, replyTo, content string) (string, error) {
	receipt, err := c.Send(ctx, channelruntime.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	return receipt.MessageID, err
}

func (c *Channel) ReactToMessage(ctx context.Context, _ string, messageID, reaction string) (func(), func(), error) {
	if !c.Running() {
		return nil, nil, errors.New("feishu: channel is not running")
	}
	if reaction == "" {
		reaction = "THUMBSUP"
	}
	emoji := larkim.NewEmojiBuilder().EmojiType(reaction).Build()
	response, err := c.client.Im.V1.MessageReaction.Create(ctx, larkim.NewCreateMessageReactionReqBuilder().MessageId(messageID).Body(larkim.NewCreateMessageReactionReqBodyBuilder().ReactionType(emoji).Build()).Build())
	if err != nil {
		return nil, nil, fmt.Errorf("feishu: create reaction: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.ReactionId == nil {
		return nil, nil, fmt.Errorf("feishu: create reaction failed: code=%d message=%s", response.Code, response.Msg)
	}
	reactionID := *response.Data.ReactionId
	remove := func() {
		_, _ = c.client.Im.V1.MessageReaction.Delete(context.Background(), larkim.NewDeleteMessageReactionReqBuilder().MessageId(messageID).ReactionId(reactionID).Build())
	}
	return func() {}, remove, nil
}

func (c *Channel) StartStreamingMessage(ctx context.Context, targetID, replyTo, content string) (string, error) {
	receipt, err := c.Send(ctx, channelruntime.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	return receipt.MessageID, err
}

func (c *Channel) UpdateStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}

func (c *Channel) FinishStreamingMessage(ctx context.Context, targetID, messageID, content string) error {
	return c.EditMessage(ctx, targetID, messageID, content)
}

func (c *Channel) sendImage(ctx context.Context, chatID string, file io.Reader) (string, error) {
	response, err := c.client.Im.V1.Image.Create(ctx, larkim.NewCreateImageReqBuilder().Body(larkim.NewCreateImageReqBodyBuilder().ImageType("message").Image(file).Build()).Build())
	if err != nil {
		return "", fmt.Errorf("feishu: upload image: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.ImageKey == nil {
		return "", fmt.Errorf("feishu: upload image failed: code=%d message=%s", response.Code, response.Msg)
	}
	content, _ := json.Marshal(map[string]string{"image_key": *response.Data.ImageKey})
	created, err := c.client.Im.V1.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(chatID).MsgType(larkim.MsgTypeImage).Content(string(content)).Build()).Build())
	if err != nil || !created.Success() {
		return "", fmt.Errorf("feishu: send image: %w", err)
	}
	if created.Data != nil && created.Data.MessageId != nil {
		return *created.Data.MessageId, nil
	}
	return "", nil
}

func (c *Channel) sendFile(ctx context.Context, chatID string, file io.Reader, filename, kind string) (string, error) {
	fileType := "stream"
	if kind == "audio" {
		fileType = "opus"
	}
	if kind == "video" {
		fileType = "mp4"
	}
	response, err := c.client.Im.V1.File.Create(ctx, larkim.NewCreateFileReqBuilder().Body(larkim.NewCreateFileReqBodyBuilder().FileType(fileType).FileName(filename).File(file).Build()).Build())
	if err != nil {
		return "", fmt.Errorf("feishu: upload file: %w", err)
	}
	if !response.Success() || response.Data == nil || response.Data.FileKey == nil {
		return "", fmt.Errorf("feishu: upload file failed: code=%d message=%s", response.Code, response.Msg)
	}
	content, _ := json.Marshal(map[string]string{"file_key": *response.Data.FileKey})
	created, err := c.client.Im.V1.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType(larkim.ReceiveIdTypeChatId).Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(chatID).MsgType(larkim.MsgTypeFile).Content(string(content)).Build()).Build())
	if err != nil || !created.Success() {
		return "", fmt.Errorf("feishu: send file: %w", err)
	}
	if created.Data != nil && created.Data.MessageId != nil {
		return *created.Data.MessageId, nil
	}
	return "", nil
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
	media := make([]channelruntime.MediaPart, 0, 1)
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
		sink(channelruntime.Inbound{ChannelID: c.config.ID, MessageID: messageID, ChatID: chatID, SenderID: senderID, Content: content, Media: media, Metadata: map[string]string{"message_type": value(message.MessageType)}, ReceivedAt: time.Now().UTC()})
	}
	return nil
}

func (c *Channel) ingestInboundImage(ctx context.Context, key string) (channelruntime.MediaPart, error) {
	if c.config.MediaStore == nil {
		return channelruntime.MediaPart{}, errors.New("feishu: media store is not configured")
	}
	response, err := c.client.Im.V1.Image.Get(ctx, larkim.NewGetImageReqBuilder().ImageKey(key).Build())
	if err != nil || !response.Success() {
		if err != nil {
			return channelruntime.MediaPart{}, err
		}
		return channelruntime.MediaPart{}, fmt.Errorf("feishu: download image failed: code=%d message=%s", response.Code, response.Msg)
	}
	part, err := c.config.MediaStore.Store(ctx, key+".png", "image/png", response.File)
	part.Type = "image"
	return part, err
}

func (c *Channel) ingestInboundFile(ctx context.Context, kind, key string, payload map[string]any) (channelruntime.MediaPart, error) {
	if c.config.MediaStore == nil {
		return channelruntime.MediaPart{}, errors.New("feishu: media store is not configured")
	}
	response, err := c.client.Im.V1.File.Get(ctx, larkim.NewGetFileReqBuilder().FileKey(key).Build())
	if err != nil || !response.Success() {
		if err != nil {
			return channelruntime.MediaPart{}, err
		}
		return channelruntime.MediaPart{}, fmt.Errorf("feishu: download file failed: code=%d message=%s", response.Code, response.Msg)
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
