package channelcore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	keelithobs "github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/service"
	khttp "github.com/keelab/keelith/transport/http"
	"github.com/keelab/keelmesh/internal/config/channelcore"
	"github.com/keelab/keelmesh/internal/domain"
	channelinfra "github.com/keelab/keelmesh/internal/infrastructure/channels"
	"github.com/keelab/keelmesh/internal/infrastructure/dependencies"
	"github.com/keelab/keelmesh/internal/infrastructure/gateclient"
	"github.com/keelab/keelmesh/internal/observability"
)

type Runtime struct {
	Config   channelcore.Config
	Metadata metadata.Policy
	Loaded   channelcore.Loaded

	Health     *health.Registry
	Telemetry  *keelithobs.Bundle
	Middleware *middleware.Bundle
	Stream     *middleware.StreamBundle

	Profile *service.Profile
	Graph   *di.Graph

	Resources  *dependencies.Resources
	HTTPServer *khttp.Server
	Channels   domain.ChannelDomain
	Gate       *gateclient.Client

	Catalog *ops.RuntimeCatalog
}

func NewRuntime(ctx context.Context, output io.Writer) (*Runtime, error) {
	loaded, err := channelcore.Loade(ctx, "configs/channelcore_config.dev.yaml")
	if err != nil {
		return nil, fmt.Errorf("load configuration: %w", err)
	}
	cfg := loaded.Runtime

	policy, err := observability.BuildObsPolicy(cfg, output)
	if err != nil {
		return nil, fmt.Errorf("build observability policy: %w", err)
	}
	telemetry := policy.Telemetry()

	logDeps, audit, err := observability.WireLogging(ctx, loaded, policy)
	if err != nil {
		return nil, fmt.Errorf("wire logging: %w", err)
	}

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

	clients, err := channelinfra.NewHTTPClients(telemetry, metadataPolicy)
	if err != nil {
		return nil, fmt.Errorf("%w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}

	stack, err := channelinfra.Build(ctx, cfg, clients, telemetry.Logger())
	if err != nil {
		return nil, fmt.Errorf("%w", errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}

	inboundServer, err := channelinfra.NewInboundServer(cfg.HTTPAddr, policy, metadataPolicy, telemetry, stack.Channels)
	if err != nil {
		return nil, fmt.Errorf("%w", errors.Join(err, stack.Domain.Stop(context.WithoutCancel(ctx)), external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))))
	}

	graph, profile, err := newServiceProfile(ctx, loaded, external, logDeps, audit, stack.Domain)
	if err != nil {
		return nil, fmt.Errorf(
			"build service profile: %w",
			errors.Join(err, stack.Domain.Stop(context.WithoutCancel(ctx)), external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))),
		)
	}

	catalog := ops.NewRuntimeCatalog()
	statuses := append(external.RuntimeStatuses(), di.RuntimeStatusRegistration("application", graph))
	statuses = append(statuses, ops.RuntimeStatusRegistration{Name: "channels", Kind: "channel", Provider: stack.Domain.RuntimeStatus})
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
				stack.Domain.Stop(context.WithoutCancel(ctx)),
				external.Shutdown(context.WithoutCancel(ctx)),
				telemetry.Shutdown(context.WithoutCancel(ctx)),
			),
		)
	}

	healthRegistry := health.NewRegistry()
	for _, candidate := range stack.Channels {
		definition := candidate.Definition()
		if !definition.Enabled {
			continue
		}
		channel := candidate
		if err := healthRegistry.Register(health.KindDependency, "channel-"+definition.ID, func(ctx context.Context) health.Result {
			if err := channel.Probe(ctx); err != nil {
				return health.Fail(fmt.Sprintf("channel %q probe failed: %v", definition.ID, err))
			}
			return health.Pass("channel probe passed")
		}); err != nil {
			return nil, fmt.Errorf("register channel health %q: %w", definition.ID, err)
		}
	}
	if err := di.RegisterHealth(healthRegistry, "application-wiring", graph); err != nil {
		return nil, fmt.Errorf(
			"register dependency graph health: %w",
			errors.Join(
				err,
				graph.Close(context.WithoutCancel(ctx)),
				stack.Domain.Stop(context.WithoutCancel(ctx)),
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
		Middleware: policy.ServerMiddleware(),
		Stream:     policy.StreamMiddleware(),
		Metadata:   metadataPolicy,
		Profile:    profile,
		Graph:      graph,
		Resources:  external,
		Catalog:    catalog,
		HTTPServer: inboundServer,
		Channels:   stack.Domain,
		Gate:       stack.Gate,
	}, nil
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
	var gateErr error
	if r.Gate != nil {
		gateErr = r.Gate.Close()
	}
	return errors.Join(graphErr, gateErr, r.Resources.Shutdown(ctx))
}
