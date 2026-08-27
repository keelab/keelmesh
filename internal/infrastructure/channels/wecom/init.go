package wecom

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
	GroupTrigger   domain.GroupTriggerPolicy
	RatePerSecond  float64
	Burst          int
	QueueSize      int
	MaxRetries     int
	MediaStore     domain.MediaDomain
	HTTPClient     *http.Client
}
type Channel struct {
	config       Config
	client       *http.Client
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
	client := cfg.HTTPClient
	if client == nil {
		var err error
		client, err = http.New(&stdhttp.Client{Timeout: 15 * time.Second}, nil, metadata.Policy{}, nil, 8<<20)
		if err != nil {
			return nil, fmt.Errorf("wecom: build HTTP client: %w", err)
		}
	}
	return &Channel{config: cfg, client: client}, nil
}
