package qq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/openapi"
	"golang.org/x/oauth2"
)

var _ domain.Channel = (*Channel)(nil)

type Config struct {
	ID            string
	Enabled       bool
	AppID         string
	AppSecret     string
	AllowFrom     []string
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config      Config
	api         openapi.OpenAPI
	tokenSource oauth2.TokenSource
	session     botgo.SessionManager
	cancel      context.CancelFunc
	sink        domain.Sink
	running     atomic.Bool
	mu          sync.Mutex
	seen        sync.Map
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.AppID) == "" || strings.TrimSpace(cfg.AppSecret) == "" {
		return nil, errors.New("qq: id, app_id and app_secret are required")
	}
	return &Channel{config: cfg}, nil
}
