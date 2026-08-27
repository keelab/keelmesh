package dingtalk

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
)

var _ domain.Channel = (*Channel)(nil)

type Config struct {
	ID            string
	Enabled       bool
	ClientID      string
	ClientSecret  string
	AllowFrom     []string
	GroupTrigger  domain.GroupTriggerPolicy
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Channel struct {
	config   Config
	stream   *client.StreamClient
	cancel   context.CancelFunc
	sink     domain.Sink
	running  atomic.Bool
	webhooks sync.Map
	mu       sync.Mutex
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.ClientID) == "" || strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("dingtalk: id, client_id and client_secret are required")
	}
	return &Channel{config: cfg}, nil
}
