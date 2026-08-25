package webhook

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
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
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config  Config
	client  *http.Client
	server  *http.Server
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
	return &Channel{config: cfg, client: &http.Client{Timeout: 15 * time.Second}}, nil
}
