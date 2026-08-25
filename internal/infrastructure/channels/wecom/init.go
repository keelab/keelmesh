package wecom

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
var _ domain.MediaChannel = (*Channel)(nil)

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
	MediaStore     domain.MediaRepository
}
type Channel struct {
	config       Config
	client       *http.Client
	server       *http.Server
	cancel       context.CancelFunc
	sink         domain.Sink
	running      atomic.Bool
	mu           sync.Mutex
	accessToken  string
	tokenExpire  time.Time
	responseURLs sync.Map
	seen         sync.Map
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
