package wecom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

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

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
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
	var in domain.Inbound
	var responseURL string
	if c.config.Kind == "wecom_aibot" {
		var msg aiMessage
		if json.Unmarshal(body, &msg) != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		in = domain.Inbound{ChannelID: c.config.ID, MessageID: msg.MsgID, ChatID: msg.ChatID, SenderID: msg.From.UserID, Content: msg.Text.Content, ReceivedAt: time.Now().UTC()}
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
			in = domain.Inbound{ChannelID: c.config.ID, MessageID: obj.ID, ChatID: obj.Chat, SenderID: obj.From, Content: obj.Content, ReceivedAt: time.Now().UTC()}
		} else {
			in = domain.Inbound{ChannelID: c.config.ID, MessageID: msg.MsgID, ChatID: msg.FromUserName, SenderID: msg.FromUserName, Content: msg.Content, Media: mediaForXML(msg), ReceivedAt: time.Now().UTC()}
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

func (c *Channel) health(w http.ResponseWriter, _ *http.Request) {
	if !c.Running() {
		http.Error(w, "not running", 503)
		return
	}
	w.WriteHeader(200)
}

func mediaForXML(msg wecomXML) []domain.MediaPartEntity {
	if msg.MediaID == "" {
		return nil
	}
	kind := strings.ToLower(msg.MsgType)
	return []domain.MediaPartEntity{{Type: kind, Ref: "wecom-media://" + msg.MediaID}}
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
