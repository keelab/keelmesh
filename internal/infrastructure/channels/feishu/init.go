package feishu

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/media"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type Config struct {
	ID                string
	Enabled           bool
	AppID             string
	AppSecret         string
	EncryptKey        string
	VerificationToken string
	AllowFrom         []string
	MediaRoot         string
	RatePerSecond     float64
	Burst             int
	QueueSize         int
	MaxRetries        int
	MediaStore        domain.MediaDomain
}

type Channel struct {
	config  Config
	client  *lark.Client
	ws      *larkws.Client
	cancel  context.CancelFunc
	sink    domain.Sink
	running atomic.Bool
	mu      sync.Mutex
	seen    sync.Map
}

func New(cfg Config) (*Channel, error) {
	if strings.TrimSpace(cfg.ID) == "" {
		return nil, errors.New("feishu: channel id is required")
	}
	if cfg.MediaStore == nil && cfg.MediaRoot != "" {
		store, err := media.NewRepository(cfg.MediaRoot)
		if err != nil {
			return nil, err
		}
		cfg.MediaStore = store
	}
	return &Channel{config: cfg, client: lark.NewClient(cfg.AppID, cfg.AppSecret)}, nil
}
