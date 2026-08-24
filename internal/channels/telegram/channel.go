package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID              string
	Enabled         bool
	Token           string
	BaseURL         string
	Proxy           string
	AllowFrom       []string
	PlaceholderText string
	MediaStore      channelruntime.MediaStore
	RatePerSecond   float64
	Burst           int
	QueueSize       int
	MaxRetries      int
}

type Channel struct {
	config  Config
	client  *http.Client
	baseURL string
	cancel  context.CancelFunc
	sink    channelruntime.Sink
	running atomic.Bool
	mu      sync.Mutex
	offset  int64
	seen    sync.Map
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
}
type update struct {
	UpdateID int64    `json:"update_id"`
	Message  *message `json:"message"`
}
type message struct {
	MessageID int         `json:"message_id"`
	Chat      chat        `json:"chat"`
	From      *user       `json:"from"`
	Text      string      `json:"text"`
	Caption   string      `json:"caption"`
	Photo     []photoSize `json:"photo"`
	Document  *document   `json:"document"`
	Audio     *audio      `json:"audio"`
	Video     *video      `json:"video"`
}
type chat struct {
	ID int64 `json:"id"`
}
type user struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}
type photoSize struct {
	FileID string `json:"file_id"`
}
type document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
}
type audio struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
}
type video struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
}
type sentMessage struct {
	MessageID int `json:"message_id"`
}
type fileInfo struct {
	FilePath string `json:"file_path"`
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("telegram: id and token are required")
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = "https://api.telegram.org"
	}
	if _, err := url.ParseRequestURI(base); err != nil {
		return nil, fmt.Errorf("telegram: invalid base url: %w", err)
	}
	return &Channel{config: cfg, client: &http.Client{Timeout: 45 * time.Second}, baseURL: base + "/bot" + cfg.Token}, nil
}

func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: "telegram", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "media", "edit", "typing", "placeholder"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }

func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("telegram: sink is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.sink = sink
	c.mu.Unlock()
	c.running.Store(true)
	go c.poll(runCtx)
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
	var result map[string]any
	return c.call(ctx, "getMe", nil, &result)
}

func (c *Channel) poll(ctx context.Context) {
	for ctx.Err() == nil {
		var updates []update
		err := c.call(ctx, "getUpdates", map[string]any{"offset": atomic.LoadInt64(&c.offset), "timeout": 30, "allowed_updates": []string{"message"}}, &updates)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
			continue
		}
		for _, item := range updates {
			atomic.StoreInt64(&c.offset, item.UpdateID+1)
			c.handleUpdate(ctx, item)
		}
	}
}

func (c *Channel) handleUpdate(ctx context.Context, item update) {
	if item.Message == nil {
		return
	}
	m := item.Message
	id := strconv.FormatInt(item.UpdateID, 10)
	if _, loaded := c.seen.LoadOrStore(id, time.Now()); loaded {
		return
	}
	senderID := ""
	senderName := ""
	if m.From != nil {
		senderID = strconv.FormatInt(m.From.ID, 10)
		senderName = strings.TrimSpace(m.From.FirstName + " " + m.From.LastName)
		if senderName == "" {
			senderName = m.From.Username
		}
	}
	if !allowed(c.config.AllowFrom, senderID, "@"+m.FromUsername()) {
		return
	}
	content := m.Text
	if content == "" {
		content = m.Caption
	}
	media := make([]channelruntime.MediaPart, 0, 1)
	if len(m.Photo) > 0 {
		if part, err := c.ingestFile(ctx, m.Photo[len(m.Photo)-1].FileID, "image", "image.jpg", "image/jpeg"); err == nil {
			media = append(media, part)
		}
	}
	if m.Document != nil {
		if part, err := c.ingestFile(ctx, m.Document.FileID, "file", m.Document.FileName, m.Document.MimeType); err == nil {
			media = append(media, part)
		}
	}
	if m.Audio != nil {
		if part, err := c.ingestFile(ctx, m.Audio.FileID, "audio", m.Audio.FileName, m.Audio.MimeType); err == nil {
			media = append(media, part)
		}
	}
	if m.Video != nil {
		if part, err := c.ingestFile(ctx, m.Video.FileID, "video", m.Video.FileName, m.Video.MimeType); err == nil {
			media = append(media, part)
		}
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(channelruntime.Inbound{ChannelID: c.config.ID, MessageID: strconv.Itoa(m.MessageID), ChatID: strconv.FormatInt(m.Chat.ID, 10), SenderID: senderID, SenderName: senderName, Content: content, Media: media, Metadata: map[string]string{"platform": "telegram"}, ReceivedAt: time.Now().UTC()})
	}
}

func (c *Channel) ingestFile(ctx context.Context, fileID, kind, filename, contentType string) (channelruntime.MediaPart, error) {
	if c.config.MediaStore == nil {
		return channelruntime.MediaPart{}, errors.New("telegram: media store is not configured")
	}
	var info fileInfo
	if err := c.call(ctx, "getFile", map[string]any{"file_id": fileID}, &info); err != nil {
		return channelruntime.MediaPart{}, err
	}
	base := strings.TrimSuffix(c.baseURL, "/bot"+c.config.Token)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/file/bot"+c.config.Token+"/"+info.FilePath, nil)
	if err != nil {
		return channelruntime.MediaPart{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return channelruntime.MediaPart{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return channelruntime.MediaPart{}, fmt.Errorf("telegram: download file returned %s", response.Status)
	}
	part, err := c.config.MediaStore.Store(ctx, filename, contentType, response.Body)
	part.Type = kind
	return part, err
}

func (m *message) FromUsername() string {
	if m.From == nil {
		return ""
	}
	return m.From.Username
}
func allowed(list []string, values ...string) bool {
	if len(list) == 0 {
		return true
	}
	for _, a := range list {
		a = strings.TrimPrefix(strings.TrimSpace(a), "@")
		for _, v := range values {
			if a != "" && (a == v || a == strings.TrimPrefix(v, "@")) {
				return true
			}
		}
	}
	return false
}

func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	params := map[string]any{"chat_id": msg.TargetID, "text": msg.Content, "parse_mode": "HTML"}
	if msg.ReplyToMessageID != "" {
		if id, err := strconv.Atoi(msg.ReplyToMessageID); err == nil {
			params["reply_parameters"] = map[string]any{"message_id": id}
		}
	}
	var result sentMessage
	if err := c.call(ctx, "sendMessage", params, &result); err != nil {
		return channelruntime.Receipt{}, err
	}
	return channelruntime.Receipt{MessageID: strconv.Itoa(result.MessageID), AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) SendMedia(ctx context.Context, msg channelruntime.OutboundMedia) (channelruntime.Receipt, error) {
	if c.config.MediaStore == nil {
		return channelruntime.Receipt{}, errors.New("telegram: media store is not configured")
	}
	var last string
	for _, part := range msg.Parts {
		if strings.HasPrefix(part.Ref, "telegram-file://") {
			return channelruntime.Receipt{}, errors.New("telegram: inbound file refs are not outbound media refs")
		}
		resource, err := c.config.MediaStore.Open(ctx, part.Ref)
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		field := "document"
		endpoint := "sendDocument"
		if part.Type == "image" {
			field = "photo"
			endpoint = "sendPhoto"
		}
		if part.Type == "video" {
			field = "video"
			endpoint = "sendVideo"
		}
		if part.Type == "audio" {
			field = "audio"
			endpoint = "sendAudio"
		}
		id, err := c.upload(ctx, endpoint, field, msg.TargetID, part.Filename, part.Caption, resource.Reader)
		_ = resource.Reader.Close()
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		last = id
	}
	return channelruntime.Receipt{MessageID: last, AcceptedAt: time.Now().UTC()}, nil
}

func (c *Channel) EditMessage(ctx context.Context, targetID, messageID, content string) error {
	id, err := strconv.Atoi(messageID)
	if err != nil {
		return err
	}
	var result sentMessage
	return c.call(ctx, "editMessageText", map[string]any{"chat_id": targetID, "message_id": id, "text": content, "parse_mode": "HTML"}, &result)
}
func (c *Channel) SendPlaceholder(ctx context.Context, targetID, replyTo, content string) (string, error) {
	if content == "" {
		content = c.config.PlaceholderText
	}
	if content == "" {
		content = "正在处理，请稍候..."
	}
	receipt, err := c.Send(ctx, channelruntime.Outbound{TargetID: targetID, ReplyToMessageID: replyTo, Content: content})
	return receipt.MessageID, err
}
func (c *Channel) StartTyping(ctx context.Context, targetID string) (func(), error) {
	if err := c.call(ctx, "sendChatAction", map[string]any{"chat_id": targetID, "action": "typing"}, &struct{}{}); err != nil {
		return nil, err
	}
	typingCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-typingCtx.Done():
				return
			case <-ticker.C:
				_ = c.call(typingCtx, "sendChatAction", map[string]any{"chat_id": targetID, "action": "typing"}, &struct{}{})
			}
		}
	}()
	return cancel, nil
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

func (c *Channel) upload(ctx context.Context, endpoint, field, target, filename, caption string, reader io.Reader) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("chat_id", target)
	if caption != "" {
		_ = mw.WriteField("caption", caption)
	}
	part, err := mw.CreateFormFile(field, filename)
	if err != nil {
		return "", err
	}
	if _, err = io.Copy(part, reader); err != nil {
		return "", err
	}
	if err = mw.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+endpoint, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var envelope apiResponse[sentMessage]
	if err = json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return "", err
	}
	if !envelope.OK {
		return "", fmt.Errorf("telegram %s failed: %s", endpoint, envelope.Description)
	}
	return strconv.Itoa(envelope.Result.MessageID), nil
}

var _ channelruntime.Channel = (*Channel)(nil)
