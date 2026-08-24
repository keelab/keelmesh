package channelcore

import (
	"context"
	"errors"
	"fmt"
	"io"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	keelithobs "github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/service"
	"github.com/keelab/keelmesh/internal/config"
	"github.com/keelab/keelmesh/internal/infrastructure/dependencies"
	"github.com/keelab/keelmesh/internal/observability"
)

type ChannelRuntime struct {
	Config   config.ChannelConfig
	Metadata metadata.Policy
	Loaded   config.ChannelLoaded

	Health     *health.Registry
	Telemetry  *keelithobs.Bundle
	Middleware *middleware.Bundle
	Stream     *middleware.StreamBundle
	Profile    *service.Profile
	Graph      *di.Graph
	Resources  *dependencies.Resources
	Catalog    *ops.RuntimeCatalog
}

func NewRuntime(ctx context.Context, output io.Writer) (*ChannelRuntime, error) {
	loaded, err := config.LoadChannel(ctx, "configs/channelcore_config.dev.yaml")
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
	graph, profile, err := newServiceProfile(ctx, loaded, external, loggingDependencies, auditLogger)
	if err != nil {
		return nil, fmt.Errorf(
			"build service profile: %w",
			errors.Join(err, external.Shutdown(context.WithoutCancel(ctx)), telemetry.Shutdown(context.WithoutCancel(ctx))),
		)
	}
	catalog := ops.NewRuntimeCatalog()
	statuses := append(external.RuntimeStatuses(), di.RuntimeStatusRegistration("application", graph))
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
	return &ChannelRuntime{
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
	}, nil
}

// Close releases construction-owned DI resources and external clients.
func (runtime *ChannelRuntime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	var graphErr error
	if runtime.Graph != nil {
		graphErr = runtime.Graph.Close(ctx)
	}
	return errors.Join(graphErr, runtime.Resources.Shutdown(ctx))
}
