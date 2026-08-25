package channelcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	keelithobs "github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/service"
	khttp "github.com/keelab/keelith/transport/http"
	"github.com/keelab/keelmesh/internal/config/channelcore"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/dingtalk"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/feishu"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/qq"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/telegram"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/webhook"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/wecom"
	"github.com/keelab/keelmesh/internal/infrastructure/dependencies"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/channel"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/media"
	"github.com/keelab/keelmesh/internal/observability"
	"github.com/keelab/keelmesh/internal/observability/sdklog"
	http2 "github.com/keelab/keelmesh/internal/transport/http"
	dingtalklogger "github.com/open-dingtalk/dingtalk-stream-sdk-go/logger"
	botgo "github.com/tencent-connect/botgo"
)

type Runtime struct {
	Config   channelcore.Config
	Metadata metadata.Policy
	Loaded   channelcore.Loaded

	Health     *health.Registry
	Telemetry  *keelithobs.Bundle
	Middleware *middleware.Bundle
	Stream     *middleware.StreamBundle
	Profile    *service.Profile
	Graph      *di.Graph
	Resources  *dependencies.Resources
	Catalog    *ops.RuntimeCatalog
	HTTPServer *khttp.Server
	Channels   domain.ChannelDomain
}

func NewRuntime(ctx context.Context, output io.Writer) (*Runtime, error) {
	loaded, err := channelcore.Loade(ctx, "configs/channelcore_config.dev.yaml")
	if err != nil {
		return nil, fmt.Errorf("load configuration: %w", err)
	}
	cfg := loaded.Runtime
	observabilityPolicy, err := observability.New(observability.Config{
		ServiceName: cfg.AppName,
		Environment: cfg.Environment,
		Logging: logging.Config{
			Level:     cfg.Logging.Level,
			Format:    cfg.Logging.Format,
			AddSource: cfg.Logging.AddSource,
		},
		Output:             output,
		RequestLogsEnabled: cfg.Observability.RequestLogs.Enabled,
		RequestLogs:        cfg.Observability.RequestLogs.Policy(),
	})
	if err != nil {
		return nil, fmt.Errorf("build observability: %w", err)
	}
	telemetry := observabilityPolicy.Telemetry()
	loggingDependencies, auditLogger, err := observabilityPolicy.LoggerDependencies()
	if err != nil {
		_ = telemetry.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("build logging dependencies: %w", err)
	}
	if err := loaded.Manager.Subscribe("logging-level", func(_ context.Context, snapshot kconfig.Snapshot) error {
		value, ok := snapshot.Lookup("runtime", "logging", "level")
		if !ok {
			return fmt.Errorf("runtime.logging.level is missing")
		}
		level, ok := value.(string)
		if !ok {
			return fmt.Errorf("runtime.logging.level is not a string")
		}
		_, applyErr := loggingDependencies.Controller.ApplyBaseline(level)
		return applyErr
	}); err != nil {
		_ = telemetry.Shutdown(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("subscribe logging level: %w", err)
	}
	// build metadata policy
	metadataPolicy, err := metadata.NewPolicy([]string{
		"x-request-id",
		"x-idempotency-key",
	})
	if err != nil {
		return nil, fmt.Errorf("build metadata policy: %w", errors.Join(err, telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	external, err := dependencies.Build(ctx, loaded, telemetry)
	if err != nil {
		return nil, fmt.Errorf("build dependencies: %w", errors.Join(err, telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	outboundBundle, err := middleware.NewBundle(middleware.Entry{
		Name:       "observability",
		Middleware: telemetry.ClientMiddleware(),
	})
	if err != nil {
		return nil, fmt.Errorf("build outbound HTTP middleware: %w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	newHTTPClient := func(timeout time.Duration) (*http2.Client, error) {
		return http2.New(
			&http.Client{Timeout: timeout},
			outboundBundle,
			metadataPolicy,
			telemetry.Propagator(),
			8<<20,
		)
	}
	telegramHTTPClient, err := newHTTPClient(45 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("build telegram HTTP client: %w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	feishuHTTPClient, err := newHTTPClient(15 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("build feishu HTTP client: %w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	webhookHTTPClient, err := newHTTPClient(15 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("build webhook HTTP client: %w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	wecomHTTPClient, err := newHTTPClient(15 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("build wecom HTTP client: %w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	sdkLogger := telemetry.Logger()
	dingtalklogger.SetLogger(sdklog.NewDingTalk(sdkLogger))
	botgo.SetLogger(sdklog.NewQQ(sdkLogger))
	channelRegistry := channel.NewRegistry()
	mediaRoot := ".data/media"
	for _, definition := range cfg.Channels {
		if strings.TrimSpace(definition.MediaRoot) != "" {
			mediaRoot = definition.MediaRoot
			break
		}
	}
	sharedMediaStore := mediaStoreFor(mediaRoot)
	for _, definition := range cfg.Channels {
		var c domain.Channel
		var buildErr error
		switch definition.Kind {
		case "feishu":
			c, buildErr = feishu.New(feishu.Config{ID: definition.ID, Enabled: definition.Enabled, AppID: definition.AppID, AppSecret: definition.AppSecret, EncryptKey: definition.EncryptKey, VerificationToken: definition.VerificationToken, AllowFrom: definition.AllowFrom, MediaRoot: definition.MediaRoot, MediaStore: sharedMediaStore, HTTPClient: feishuHTTPClient, Logger: sdklog.NewFeishu(sdkLogger), RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "telegram":
			c, buildErr = telegram.New(telegram.Config{ID: definition.ID, Enabled: definition.Enabled, Token: definition.Telegram.Token, BaseURL: definition.Telegram.BaseURL, Proxy: definition.Telegram.Proxy, AllowFrom: definition.Telegram.AllowFrom, PlaceholderText: definition.Telegram.Placeholder.Text, MediaStore: sharedMediaStore, HTTPClient: telegramHTTPClient, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "webhook":
			c, buildErr = webhook.New(webhook.Config{ID: definition.ID, Enabled: definition.Enabled, OutboundURL: definition.Webhook.OutboundURL, Listen: definition.Webhook.Listen, Path: definition.Webhook.Path, Secret: definition.Webhook.Secret, AllowFrom: definition.Webhook.AllowFrom, MediaStore: sharedMediaStore, HTTPClient: webhookHTTPClient, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "dingtalk":
			c, buildErr = dingtalk.New(dingtalk.Config{ID: definition.ID, Enabled: definition.Enabled, ClientID: definition.DingTalk.ClientID, ClientSecret: definition.DingTalk.ClientSecret, AllowFrom: definition.DingTalk.AllowFrom, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "qq":
			c, buildErr = qq.New(qq.Config{ID: definition.ID, Enabled: definition.Enabled, AppID: definition.QQ.AppID, AppSecret: definition.QQ.AppSecret, AllowFrom: definition.QQ.AllowFrom, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "wecom":
			c, buildErr = wecom.New(wecom.Config{ID: definition.ID, Kind: "wecom", Enabled: definition.Enabled, WebhookURL: definition.WeCom.WebhookURL, Token: definition.WeCom.Token, EncodingAESKey: definition.WeCom.EncodingAESKey, Listen: definition.WeCom.WebhookListen, Path: definition.WeCom.WebhookPath, AllowFrom: definition.WeCom.AllowFrom, MediaStore: sharedMediaStore, HTTPClient: wecomHTTPClient, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "wecom_app":
			c, buildErr = wecom.New(wecom.Config{ID: definition.ID, Kind: "wecom_app", Enabled: definition.Enabled, CorpID: definition.WeComApp.CorpID, CorpSecret: definition.WeComApp.CorpSecret, AgentID: definition.WeComApp.AgentID, Token: definition.WeComApp.Token, EncodingAESKey: definition.WeComApp.EncodingAESKey, Listen: definition.WeComApp.WebhookListen, Path: definition.WeComApp.WebhookPath, AllowFrom: definition.WeComApp.AllowFrom, MediaStore: sharedMediaStore, HTTPClient: wecomHTTPClient, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		case "wecom_aibot":
			c, buildErr = wecom.New(wecom.Config{ID: definition.ID, Kind: "wecom_aibot", Enabled: definition.Enabled, Token: definition.WeComAIBot.Token, EncodingAESKey: definition.WeComAIBot.EncodingAESKey, Listen: definition.WeComAIBot.WebhookListen, Path: definition.WeComAIBot.WebhookPath, AllowFrom: definition.WeComAIBot.AllowFrom, MediaStore: sharedMediaStore, HTTPClient: wecomHTTPClient, RatePerSecond: definition.RatePerSecond, Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries})
		default:
			if definition.Enabled {
				return nil, fmt.Errorf("unsupported enabled channel kind %q", definition.Kind)
			}
			continue
		}
		if buildErr != nil {
			return nil, fmt.Errorf("build channel %q: %w", definition.ID, buildErr)
		}
		if err := channelRegistry.Register(c); err != nil {
			return nil, fmt.Errorf("register channel %q: %w", definition.ID, err)
		}
	}
	channels, err := channel.NewRepository(channelRegistry)
	if err != nil {
		return nil, fmt.Errorf("build channel runtime: %w", err)
	}
	if sharedMediaStore != nil {
		channels.SetMediaStore(sharedMediaStore)
	}
	inboundRouter, err := khttp.NewRouter(
		khttp.WithMiddleware(observabilityPolicy.ServerMiddleware()),
		khttp.WithMetadataPolicy(metadataPolicy),
		khttp.WithPropagator(telemetry.Propagator()),
		khttp.WithMaxResponseBytes(1<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("build channel HTTP router: %w", errors.Join(err, channels.Stop(context.WithoutCancel(ctx)), external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	for _, candidate := range channelRegistry.All() {
		registrar, ok := candidate.(http2.Registrar)
		if !ok {
			continue
		}
		if err := registrar.RegisterHTTP(inboundRouter); err != nil {
			return nil, fmt.Errorf("register channel HTTP route: %w", errors.Join(err, channels.Stop(context.WithoutCancel(ctx)), external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
		}
	}
	inboundServer, err := khttp.NewServer(
		inboundRouter,
		khttp.WithName("channelcore-http"),
		khttp.WithAddress(cfg.HTTPAddr),
		khttp.WithMaxRequestBodyBytes(8<<20),
		khttp.WithReadHeaderTimeout(5*time.Second),
		khttp.WithReadTimeout(30*time.Second),
		khttp.WithWriteTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("build channel HTTP server: %w", errors.Join(err, channels.Stop(context.WithoutCancel(ctx)), external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}
	graph, profile, err := newServiceProfile(ctx, loaded, external, loggingDependencies, auditLogger, channels)
	if err != nil {
		return nil, fmt.Errorf(
			"build service profile: %w",
			errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))),
		)
	}
	catalog := ops.NewRuntimeCatalog()
	statuses := append(external.RuntimeStatuses(), di.RuntimeStatusRegistration("application", graph))
	statuses = append(statuses, ops.RuntimeStatusRegistration{Name: "channels", Kind: "channel", Provider: channels.RuntimeStatus})
	statuses = append(statuses,
		ops.RuntimeStatusRegistration{Name: "application", Kind: "logging", Provider: ops.LoggingRuntimeStatus(telemetry.LoggingController())},
		ops.RuntimeStatusRegistration{Name: "audit", Kind: "logging", Provider: ops.AuditRuntimeStatus(telemetry.AuditLogger())},
	)
	if err := catalog.RegisterAll(statuses...); err != nil {
		return nil, fmt.Errorf(
			"register dependency diagnostics: %w",
			errors.Join(
				err,
				graph.Close(context.WithoutCancel(ctx)),
				external.Shutdown(context.WithoutCancel(ctx)),
				telemetry.Shutdown(context.WithoutCancel(ctx)),
			),
		)
	}
	healthRegistry := health.NewRegistry()
	if err := di.RegisterHealth(healthRegistry, "application-wiring", graph); err != nil {
		return nil, fmt.Errorf(
			"register dependency graph health: %w",
			errors.Join(
				err,
				graph.Close(context.WithoutCancel(ctx)),
				external.Shutdown(context.WithoutCancel(ctx)),
				telemetry.Shutdown(context.WithoutCancel(ctx)),
			),
		)
	}
	return &Runtime{
		Config:     cfg,
		Loaded:     loaded,
		Health:     healthRegistry,
		Telemetry:  telemetry,
		Middleware: observabilityPolicy.ServerMiddleware(),
		Stream:     observabilityPolicy.StreamMiddleware(),
		Metadata:   metadataPolicy,
		Profile:    profile,
		Graph:      graph,
		Resources:  external,
		Catalog:    catalog,
		HTTPServer: inboundServer,
		Channels:   channels,
	}, nil
}

func mediaStoreFor(root string) domain.MediaDomain {
	if root == "" {
		return nil
	}
	store, err := media.NewRepository(root)
	if err != nil {
		return nil
	}
	return store
}

// Close releases construction-owned DI resources and external clients.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var graphErr error
	if r.Graph != nil {
		graphErr = r.Graph.Close(ctx)
	}
	return errors.Join(graphErr, r.Resources.Shutdown(ctx))
}
