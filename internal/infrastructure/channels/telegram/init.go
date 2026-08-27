package telegram

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/transport/http"
)

var _ domain.Channel = (*Channel)(nil)
var _ domain.MediaChannel = (*Channel)(nil)

type Config struct {
	ID              string
	Enabled         bool
	Token           string
	BaseURL         string
	Proxy           string
	AllowFrom       []string
	GroupTrigger    domain.GroupTriggerPolicy
	PlaceholderText string
	MediaStore      domain.MediaDomain
	HTTPClient      *http.Client
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
	client := cfg.HTTPClient
	if client == nil {
		var err error
		client, err = http.New(&stdhttp.Client{Timeout: 45 * time.Second}, nil, metadata.Policy{}, nil, 8<<20)
		if err != nil {
			return nil, fmt.Errorf("telegram: build HTTP client: %w", err)
		}
	}
	return &Channel{config: cfg, client: client, baseURL: base + "/bot" + cfg.Token}, nil
}
