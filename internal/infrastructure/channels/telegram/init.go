package telegram

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

var _ domain.Channel = (*Channel)(nil)

type Config struct {
	ID              string
	Enabled         bool
	Token           string
	BaseURL         string
	Proxy           string
	AllowFrom       []string
	PlaceholderText string
	MediaStore      domain.MediaDomain
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
	sink    domain.Sink
	running atomic.Bool
	mu      sync.Mutex
	offset  int64
	seen    sync.Map
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
