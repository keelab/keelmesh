package channels

import (
	"fmt"
	stdhttp "net/http"
	"time"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	keelithobs "github.com/keelab/keelith/observability"
	khttp "github.com/keelab/keelith/transport/http"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/observability"
	"github.com/keelab/keelmesh/internal/transport/http"
)

const (
	defaultHTTPClientMaxResponseBytes int64 = 8 << 20
	inboundHTTPMaxResponseBytes       int64 = 1 << 20
	inboundHTTPMaxRequestBodyBytes    int64 = 8 << 20
)

// HTTPClients holds outbound HTTP clients shared by channel adapters.
type HTTPClients struct {
	Telegram *http.Client
	Feishu   *http.Client
	Webhook  *http.Client
	WeCom    *http.Client
}

type httpClientSpec struct {
	name    string
	timeout time.Duration
	target  **http.Client
}

func NewHTTPClients(telemetry *keelithobs.Bundle, metadataPolicy metadata.Policy) (*HTTPClients, error) {
	outboundBundle, err := middleware.NewBundle(middleware.Entry{
		Name:       "observability",
		Middleware: telemetry.ClientMiddleware(),
	})
	if err != nil {
		return nil, fmt.Errorf("build outbound HTTP middleware: %w", err)
	}
	newClient := func(name string, timeout time.Duration) (*http.Client, error) {
		client, err := http.New(
			&stdhttp.Client{Timeout: timeout},
			outboundBundle,
			metadataPolicy,
			telemetry.Propagator(),
			defaultHTTPClientMaxResponseBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("build %s HTTP client: %w", name, err)
		}
		return client, nil
	}

	clients := &HTTPClients{}
	for _, spec := range []httpClientSpec{
		{name: "telegram", timeout: 45 * time.Second, target: &clients.Telegram},
		{name: "feishu", timeout: 15 * time.Second, target: &clients.Feishu},
		{name: "webhook", timeout: 15 * time.Second, target: &clients.Webhook},
		{name: "wecom", timeout: 15 * time.Second, target: &clients.WeCom},
	} {
		client, err := newClient(spec.name, spec.timeout)
		if err != nil {
			return nil, err
		}
		*spec.target = client
	}
	return clients, nil
}

func NewInboundServer(addr string, policy *observability.Bundle, metadataPolicy metadata.Policy, telemetry *keelithobs.Bundle, channels []domain.Channel) (*khttp.Server, error) {
	router, err := khttp.NewRouter(
		khttp.WithMiddleware(policy.ServerMiddleware()),
		khttp.WithMetadataPolicy(metadataPolicy),
		khttp.WithPropagator(telemetry.Propagator()),
		khttp.WithMaxResponseBytes(inboundHTTPMaxResponseBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("build channel HTTP router: %w", err)
	}
	for _, candidate := range channels {
		registrar, ok := candidate.(http.Registrar)
		if !ok {
			continue
		}
		if err = registrar.RegisterHTTP(router); err != nil {
			return nil, fmt.Errorf("register channel HTTP route: %w", err)
		}
	}
	server, err := khttp.NewServer(
		router,
		khttp.WithName("channelcore-http"),
		khttp.WithAddress(addr),
		khttp.WithMaxRequestBodyBytes(inboundHTTPMaxRequestBodyBytes),
		khttp.WithReadHeaderTimeout(5*time.Second),
		khttp.WithReadTimeout(30*time.Second),
		khttp.WithWriteTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("build channel HTTP server: %w", err)
	}
	return server, nil
}
