package channelcore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/ops"
	kserver "github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
)

type appServer interface {
	kserver.Server
	kserver.Waiter
	kserver.Named
}

func buildApp(ctx context.Context, runtime *ChannelRuntime, apiServer appServer, opsAddress string, surface *service.Surface) error {
	registry, err := service.NewSurfaceRegistry(surface)
	if err != nil {
		_ = runtime.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("build listener registry: %w", err)
	}
	opsOptions := []ops.Option{
		ops.WithAddress(opsAddress),
		ops.WithConfigStatus(ops.ConfigManagerStatus(runtime.Loaded.Manager)),
		ops.WithRuntimeStatus(ops.RuntimeCatalogStatus(runtime.Catalog)),
		ops.WithServiceProfiles(service.DiagnosticHandlerFromRegistry(runtime.Profile, registry)),
	}
	if runtime.Config.OpsHealthOnly {
		opsOptions = append(opsOptions, ops.WithAccessPolicy(func(*http.Request) error {
			return fmt.Errorf("ops diagnostics are disabled")
		}))
	} else {
		opsOptions = append(opsOptions,
			ops.WithPrincipalAccessPolicy(func(request *http.Request) (ops.Principal, error) {
				host, _, splitErr := net.SplitHostPort(request.RemoteAddr)
				if splitErr != nil || !net.ParseIP(host).IsLoopback() {
					return ops.Principal{}, fmt.Errorf("ops request is not loopback")
				}
				return ops.Principal{Subject: "local-operator"}, nil
			}),
			ops.WithLoggingAdmin(ops.LoggingAdminConfig{
				Controller: runtime.Telemetry.LoggingController(),
				Audit:      runtime.Telemetry.AuditLogger(),
			}),
		)
	}
	opsServer, err := ops.New(runtime.Health, opsOptions...)
	if err != nil {
		_ = runtime.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("build ops server: %w", err)
	}
	backgroundServers := runtime.Resources.Servers()
	servers := make([]kserver.Server, 0)
	servers = append(servers, runtime.Loaded.RuntimeServer)
	servers = append(servers, backgroundServers...)
	observed, observeErr := service.ObserveListener(
		apiServer,
		surface,
		runtime.Telemetry.Logger(),
	)
	if observeErr != nil {
		_ = runtime.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("observe API listener: %w", observeErr)
	}
	servers = append(servers, observed)
	servers = append(servers, opsServer)
	application, err := app.New(
		app.WithHealth(runtime.Health),
		app.WithLifecycles(runtime.Telemetry),
		app.WithComponents(runtime.Resources.Components()...),
		di.AppOption(runtime.Graph),
		app.WithServers(servers...),
		app.WithStopTimeout(runtime.Config.ShutdownTimeout),
	)
	if err != nil {
		_ = runtime.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("build application: %w", err)
	}
	if err = runtime.Catalog.Register(
		"application",
		"app",
		ops.ApplicationRuntimeStatus(application),
	); err != nil {
		_ = runtime.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("register application diagnostics: %w", err)
	}
	runErr := application.Run(ctx)
	return errors.Join(runErr, runtime.Close(context.WithoutCancel(ctx)))
}
