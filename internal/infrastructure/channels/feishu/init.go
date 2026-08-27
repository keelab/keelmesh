package feishu

import (
	"bytes"
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/media"
	"github.com/keelab/keelmesh/internal/transport/http"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

var _ domain.Channel = (*Channel)(nil)
var _ domain.MediaChannel = (*Channel)(nil)

type Config struct {
	ID                string
	Enabled           bool
	AppID             string
	AppSecret         string
	EncryptKey        string
	VerificationToken string
	AllowFrom         []string
	GroupTrigger      domain.GroupTriggerPolicy
	MediaRoot         string
	RatePerSecond     float64
	Burst             int
	QueueSize         int
	MaxRetries        int
	MediaStore        domain.MediaDomain
	HTTPClient        *http.Client
	Logger            larkcore.Logger
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
	options := make([]lark.ClientOptionFunc, 0, 2)
	if cfg.HTTPClient != nil {
		options = append(options, lark.WithHttpClient(&larkHTTPClient{client: cfg.HTTPClient}))
	}
	if cfg.Logger != nil {
		options = append(options, lark.WithLogger(cfg.Logger))
	}
	return &Channel{config: cfg, client: lark.NewClient(cfg.AppID, cfg.AppSecret, options...)}, nil
}

type larkHTTPClient struct {
	client *http.Client
}

func (c *larkHTTPClient) Do(request *stdhttp.Request) (*stdhttp.Response, error) {
	return http.Do[*stdhttp.Response](request.Context(), c.client, "feishu", request.Method, request, func(_ context.Context, response *stdhttp.Response) (*stdhttp.Response, error) {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		cloned := new(stdhttp.Response)
		*cloned = *response
		cloned.Body = io.NopCloser(bytes.NewReader(body))
		cloned.ContentLength = int64(len(body))
		return cloned, nil
	})
}

var _ larkcore.HttpClient = (*larkHTTPClient)(nil)
