package webhook

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/transport/http"
)

var _ domain.Channel = (*Channel)(nil)

type Config struct {
	ID            string
	Enabled       bool
	OutboundURL   string
	Listen        string
	Path          string
	Secret        string
	AllowFrom     []string
	MediaStore    domain.MediaDomain
	HTTPClient    *http.Client
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config  Config
	client  *http.Client
	cancel  context.CancelFunc
	sink    domain.Sink
	running atomic.Bool
	mu      sync.Mutex
	seen    sync.Map
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("webhook: id is required")
	}
	if cfg.Path == "" {
		cfg.Path = "/webhook/" + cfg.ID
	}
	client := cfg.HTTPClient
	if client == nil {
		var err error
		client, err = http.New(&stdhttp.Client{Timeout: 15 * time.Second}, nil, metadata.Policy{}, nil, 8<<20)
		if err != nil {
			return nil, fmt.Errorf("webhook: build HTTP client: %w", err)
		}
	}
	return &Channel{config: cfg, client: client}, nil
}
