package telegram

import (
	"context"
	"errors"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/transport/http"
)

type update struct {
	UpdateID int64    `json:"update_id"`
	Message  *message `json:"message"`
}
type message struct {
	MessageID int `json:"message_id"`
	Chat      struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	From *struct {
		ID        int64  `json:"id"`
		Username  string `json:"username"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
	} `json:"from"`
	Text    string `json:"text"`
	Caption string `json:"caption"`
	Photo   []struct {
		FileID string `json:"file_id"`
	} `json:"photo"`
	Document *struct {
		FileID   string `json:"file_id"`
		FileName string `json:"file_name"`
		MimeType string `json:"mime_type"`
	} `json:"document"`
	Audio *struct {
		FileID   string `json:"file_id"`
		FileName string `json:"file_name"`
		MimeType string `json:"mime_type"`
	} `json:"audio"`
	Video *struct {
		FileID   string `json:"file_id"`
		FileName string `json:"file_name"`
		MimeType string `json:"mime_type"`
	} `json:"video"`
}

func (c *Channel) Start(ctx context.Context, sink domain.Sink) error {
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
	media := make([]domain.MediaPartEntity, 0, 1)
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
		scope := "direct"
		if m.Chat.Type == "group" || m.Chat.Type == "supergroup" {
			scope = "group"
		}
		sink(domain.Inbound{ChannelID: c.config.ID, MessageID: strconv.Itoa(m.MessageID), ChatID: strconv.FormatInt(m.Chat.ID, 10), SenderID: senderID, SenderName: senderName, Content: content, Media: media, Metadata: map[string]string{"platform": "telegram", "scope": scope}, ReceivedAt: time.Now().UTC()})
	}
}

func (c *Channel) ingestFile(ctx context.Context, fileID, kind, filename, contentType string) (domain.MediaPartEntity, error) {
	if c.config.MediaStore == nil {
		return domain.MediaPartEntity{}, errors.New("telegram: media store is not configured")
	}
	var info fileInfo
	if err := c.call(ctx, "getFile", map[string]any{"file_id": fileID}, &info); err != nil {
		return domain.MediaPartEntity{}, err
	}
	base := strings.TrimSuffix(c.baseURL, "/bot"+c.config.Token)
	request, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, base+"/file/bot"+c.config.Token+"/"+info.FilePath, nil)
	if err != nil {
		return domain.MediaPartEntity{}, err
	}
	value, err := http.Do[domain.MediaPartEntity](ctx, c.client, "telegram", "downloadFile", request,
		func(_ context.Context, response *stdhttp.Response) (domain.MediaPartEntity, error) {
			part, err := c.config.MediaStore.Store(ctx, filename, contentType, response.Body)
			if err != nil {
				return domain.MediaPartEntity{}, err
			}
			part.Type = kind
			return part, nil
		})
	if err != nil {
		return domain.MediaPartEntity{}, err
	}
	return value, nil
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
