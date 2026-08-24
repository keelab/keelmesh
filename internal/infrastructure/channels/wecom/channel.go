package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
)

type Config struct {
	ID             string
	Kind           string
	Enabled        bool
	WebhookURL     string
	CorpID         string
	CorpSecret     string
	AgentID        int64
	Token          string
	EncodingAESKey string
	Listen         string
	Path           string
	AllowFrom      []string
	RatePerSecond  float64
	Burst          int
	QueueSize      int
	MaxRetries     int
	MediaStore     channelruntime.MediaStore
}

type Channel struct {
	config       Config
	client       *http.Client
	server       *http.Server
	cancel       context.CancelFunc
	sink         channelruntime.Sink
	running      atomic.Bool
	mu           sync.Mutex
	accessToken  string
	tokenExpire  time.Time
	responseURLs sync.Map
	seen         sync.Map
}
type wecomXML struct {
	MsgType      string `xml:"MsgType"`
	FromUserName string `xml:"FromUserName"`
	ToUserName   string `xml:"ToUserName"`
	Content      string `xml:"Content"`
	MsgID        string `xml:"MsgId"`
	MediaID      string `xml:"MediaId"`
	Event        string `xml:"Event"`
}
type aiMessage struct {
	MsgID       string `json:"msgid"`
	ChatID      string `json:"chatid"`
	ResponseURL string `json:"response_url"`
	From        struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("wecom: id is required")
	}
	if cfg.Kind == "" {
		cfg.Kind = "wecom"
	}
	if cfg.Path == "" {
		cfg.Path = "/webhook/" + cfg.ID
	}
	if cfg.Kind == "wecom" && strings.TrimSpace(cfg.WebhookURL) == "" {
		return nil, errors.New("wecom: webhook_url is required")
	}
	if cfg.Kind == "wecom_app" && (cfg.CorpID == "" || cfg.CorpSecret == "" || cfg.AgentID == 0) {
		return nil, errors.New("wecom_app: corp_id, corp_secret and agent_id are required")
	}
	return &Channel{config: cfg, client: &http.Client{Timeout: 15 * time.Second}}, nil
}
func (c *Channel) Definition() channelruntime.Definition {
	return channelruntime.Definition{ID: c.config.ID, Kind: c.config.Kind, Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "webhook"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
func (c *Channel) Running() bool { return c.running.Load() }
func (c *Channel) Start(ctx context.Context, sink channelruntime.Sink) error {
	if !c.config.Enabled {
		return nil
	}
	if sink == nil {
		return errors.New("wecom: sink is required")
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
		c.server = &http.Server{Addr: c.config.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := c.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				c.running.Store(false)
			}
		}()
	}
	c.running.Store(true)
	if c.config.Kind == "wecom_app" {
		go c.refreshTokenLoop(runCtx)
	}
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
	if c.config.Kind == "wecom_app" {
		_, err := c.getToken(ctx)
		return err
	}
	if !c.Running() {
		return errors.New("wecom: listener is not running")
	}
	return nil
}
func (c *Channel) Send(ctx context.Context, msg channelruntime.Outbound) (channelruntime.Receipt, error) {
	switch c.config.Kind {
	case "wecom":
		body := map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": msg.Content}}
		if err := c.postJSON(ctx, c.config.WebhookURL, body); err != nil {
			return channelruntime.Receipt{}, err
		}
	case "wecom_app":
		token, err := c.getToken(ctx)
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		body := map[string]any{"touser": msg.TargetID, "msgtype": "markdown", "agentid": c.config.AgentID, "markdown": map[string]string{"content": msg.Content}}
		endpoint := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
		if err := c.postJSON(ctx, endpoint, body); err != nil {
			return channelruntime.Receipt{}, err
		}
	case "wecom_aibot":
		raw, ok := c.responseURLs.Load(msg.TargetID)
		if !ok {
			return channelruntime.Receipt{}, errors.New("wecom_aibot: response url is unavailable")
		}
		endpoint, _ := raw.(string)
		if err := c.postJSON(ctx, endpoint, map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": msg.Content}}); err != nil {
			return channelruntime.Receipt{}, err
		}
	}
	return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
}
func (c *Channel) SendMedia(ctx context.Context, msg channelruntime.OutboundMedia) (channelruntime.Receipt, error) {
	if c.config.MediaStore == nil {
		return channelruntime.Receipt{}, errors.New("wecom: media store is not configured")
	}
	if c.config.Kind == "wecom_app" {
		token, err := c.getToken(ctx)
		if err != nil {
			return channelruntime.Receipt{}, err
		}
		for _, part := range msg.Parts {
			resource, openErr := c.config.MediaStore.Open(ctx, part.Ref)
			if openErr != nil {
				return channelruntime.Receipt{}, openErr
			}
			mediaID, uploadErr := c.uploadMedia(ctx, token, part, resource)
			_ = resource.Reader.Close()
			if uploadErr != nil {
				return channelruntime.Receipt{}, uploadErr
			}
			msgType := "file"
			if part.Type == "image" {
				msgType = "image"
			}
			payload := map[string]any{"touser": msg.TargetID, "msgtype": msgType, "agentid": c.config.AgentID}
			if msgType == "image" {
				payload["image"] = map[string]string{"media_id": mediaID}
			} else {
				payload["file"] = map[string]string{"media_id": mediaID}
			}
			endpoint := "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
			if err := c.postJSON(ctx, endpoint, payload); err != nil {
				return channelruntime.Receipt{}, err
			}
		}
		return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
	}
	if c.config.Kind == "wecom" {
		parts := make([]string, 0, len(msg.Parts))
		for _, part := range msg.Parts {
			if part.Type == "image" {
				resource, err := c.config.MediaStore.Open(ctx, part.Ref)
				if err != nil {
					return channelruntime.Receipt{}, err
				}
				data, readErr := io.ReadAll(io.LimitReader(resource.Reader, 10<<20))
				_ = resource.Reader.Close()
				if readErr != nil {
					return channelruntime.Receipt{}, readErr
				}
				hash := md5.Sum(data)
				if err := c.postJSON(ctx, c.config.WebhookURL, map[string]any{"msgtype": "image", "image": map[string]string{"base64": base64.StdEncoding.EncodeToString(data), "md5": fmt.Sprintf("%x", hash)}}); err != nil {
					return channelruntime.Receipt{}, err
				}
				continue
			}
			parts = append(parts, fmt.Sprintf("[%s] %s", part.Type, part.Ref))
		}
		if len(parts) == 0 {
			return channelruntime.Receipt{MessageID: msg.ID, AcceptedAt: time.Now().UTC()}, nil
		}
		return c.Send(ctx, channelruntime.Outbound{ID: msg.ID, ChannelID: msg.ChannelID, TargetID: msg.TargetID, Content: strings.Join(parts, "\n")})
	}
	parts := make([]string, 0, len(msg.Parts))
	for _, part := range msg.Parts {
		parts = append(parts, fmt.Sprintf("[%s] %s", part.Type, part.Ref))
	}
	return c.Send(ctx, channelruntime.Outbound{ID: msg.ID, ChannelID: msg.ChannelID, TargetID: msg.TargetID, Content: strings.Join(parts, "\n")})
}

func (c *Channel) uploadMedia(ctx context.Context, token string, part channelruntime.MediaPart, resource channelruntime.MediaResource) (string, error) {
	mediaType := "file"
	if part.Type == "image" {
		mediaType = "image"
	}
	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token=" + url.QueryEscape(token) + "&type=" + mediaType
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("media", part.Filename)
	if err != nil {
		return "", err
	}
	if _, err = io.Copy(file, resource.Reader); err != nil {
		return "", err
	}
	if err = writer.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MediaID string `json:"media_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.ErrCode != 0 || result.MediaID == "" {
		return "", fmt.Errorf("wecom: media upload failed: %s", result.ErrMsg)
	}
	return result.MediaID, nil
}
func (c *Channel) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		value := r.URL.Query().Get("echostr")
		if value != "" {
			if decrypted, err := c.decryptIncoming(value, r); err == nil {
				value = decrypted
			} else {
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(value))
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	if encrypted, err := c.decryptBody(body, r); err != nil {
		http.Error(w, "invalid encrypted message", http.StatusUnauthorized)
		return
	} else if encrypted != nil {
		body = encrypted
	}
	var in channelruntime.Inbound
	var responseURL string
	if c.config.Kind == "wecom_aibot" {
		var msg aiMessage
		if json.Unmarshal(body, &msg) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		in = channelruntime.Inbound{ChannelID: c.config.ID, MessageID: msg.MsgID, ChatID: msg.ChatID, SenderID: msg.From.UserID, Content: msg.Text.Content, ReceivedAt: time.Now().UTC()}
		responseURL = msg.ResponseURL
	} else {
		var msg wecomXML
		if xml.Unmarshal(body, &msg) != nil {
			var obj struct {
				From    string `json:"from_user"`
				Chat    string `json:"chat_id"`
				Content string `json:"content"`
				ID      string `json:"message_id"`
			}
			if json.Unmarshal(body, &obj) != nil {
				http.Error(w, "invalid message", 400)
				return
			}
			in = channelruntime.Inbound{ChannelID: c.config.ID, MessageID: obj.ID, ChatID: obj.Chat, SenderID: obj.From, Content: obj.Content, ReceivedAt: time.Now().UTC()}
		} else {
			in = channelruntime.Inbound{ChannelID: c.config.ID, MessageID: msg.MsgID, ChatID: msg.FromUserName, SenderID: msg.FromUserName, Content: msg.Content, Media: mediaForXML(msg), ReceivedAt: time.Now().UTC()}
		}
	}
	if in.MessageID != "" {
		if _, loaded := c.seen.LoadOrStore(in.MessageID, time.Now()); loaded {
			w.WriteHeader(200)
			return
		}
	}
	if !allowed(c.config.AllowFrom, in.SenderID) {
		w.WriteHeader(200)
		return
	}
	if responseURL != "" {
		c.responseURLs.Store(in.ChatID, responseURL)
	}
	c.mu.Lock()
	sink := c.sink
	c.mu.Unlock()
	if sink != nil {
		sink(in)
	}
	w.WriteHeader(200)
}

func (c *Channel) decryptIncoming(value string, r *http.Request) (string, error) {
	if c.config.Token != "" && !validSignature(c.config.Token, r.URL.Query().Get("msg_signature"), r.URL.Query().Get("timestamp"), r.URL.Query().Get("nonce"), value) {
		return "", errors.New("wecom: invalid signature")
	}
	if c.config.EncodingAESKey == "" {
		return value, nil
	}
	receiveID := ""
	if c.config.Kind == "wecom_app" {
		receiveID = c.config.CorpID
	}
	return decryptFrame(value, c.config.EncodingAESKey, receiveID)
}

func (c *Channel) decryptBody(body []byte, r *http.Request) ([]byte, error) {
	if c.config.EncodingAESKey == "" {
		return nil, nil
	}
	if c.config.Kind == "wecom_aibot" {
		var wrapper struct {
			Encrypt string `json:"encrypt"`
		}
		if err := json.Unmarshal(body, &wrapper); err != nil || wrapper.Encrypt == "" {
			return nil, errors.New("wecom: invalid encrypted json")
		}
		value, err := c.decryptIncoming(wrapper.Encrypt, r)
		return []byte(value), err
	}
	var wrapper struct {
		Encrypt string `xml:"Encrypt"`
	}
	if err := xml.Unmarshal(body, &wrapper); err != nil || wrapper.Encrypt == "" {
		return nil, errors.New("wecom: invalid encrypted xml")
	}
	value, err := c.decryptIncoming(wrapper.Encrypt, r)
	return []byte(value), err
}

func validSignature(token, signature, timestamp, nonce, encrypted string) bool {
	values := []string{token, timestamp, nonce, encrypted}
	sort.Strings(values)
	hash := sha1.Sum([]byte(strings.Join(values, "")))
	return strings.EqualFold(fmt.Sprintf("%x", hash), signature)
}

func decryptFrame(encrypted, encodedKey, receiveID string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey + "=")
	if err != nil || len(key) != 32 {
		return "", errors.New("wecom: invalid aes key")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return "", errors.New("wecom: invalid ciphertext")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plain, ciphertext)
	padding := int(plain[len(plain)-1])
	if padding <= 0 || padding > 32 || padding > len(plain) {
		return "", errors.New("wecom: invalid padding")
	}
	plain = plain[:len(plain)-padding]
	if len(plain) < 20 {
		return "", errors.New("wecom: decrypted frame too short")
	}
	length := binary.BigEndian.Uint32(plain[16:20])
	if int(length) > len(plain)-20 {
		return "", errors.New("wecom: invalid frame length")
	}
	message := plain[20 : 20+length]
	if receiveID != "" && string(plain[20+length:]) != receiveID {
		return "", errors.New("wecom: receive id mismatch")
	}
	return string(message), nil
}
func mediaForXML(msg wecomXML) []channelruntime.MediaPart {
	if msg.MediaID == "" {
		return nil
	}
	kind := strings.ToLower(msg.MsgType)
	return []channelruntime.MediaPart{{Type: kind, Ref: "wecom-media://" + msg.MediaID}}
}
func (c *Channel) health(w http.ResponseWriter, _ *http.Request) {
	if !c.Running() {
		http.Error(w, "not running", 503)
		return
	}
	w.WriteHeader(200)
}
func (c *Channel) postJSON(ctx context.Context, endpoint string, value any) error {
	body, _ := json.Marshal(value)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("wecom: endpoint returned %s", resp.Status)
	}
	return nil
}
func (c *Channel) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.accessToken != "" && time.Now().Before(c.tokenExpire) {
		token := c.accessToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()
	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=" + url.QueryEscape(c.config.CorpID) + "&corpsecret=" + url.QueryEscape(c.config.CorpSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return "", fmt.Errorf("wecom: gettoken failed: %s", result.ErrMsg)
	}
	c.mu.Lock()
	c.accessToken = result.AccessToken
	c.tokenExpire = time.Now().Add(time.Duration(result.ExpiresIn-60) * time.Second)
	c.mu.Unlock()
	return result.AccessToken, nil
}
func (c *Channel) refreshTokenLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = c.getToken(ctx)
		}
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

var _ channelruntime.Channel = (*Channel)(nil)
var _ channelruntime.MediaChannel = (*Channel)(nil)
