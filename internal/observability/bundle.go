package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/keelab/keelith/middleware"
	keelithobs "github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	kresource "github.com/keelab/keelith/observability/resource"
)

// Config contains the application-owned policy used to construct one
// App-scoped Keelith observability bundle.
type Config struct {
	ServiceName        string
	Environment        string
	Logging            logging.Config
	Output             io.Writer
	RequestLogsEnabled bool
	RequestLogs        logging.RequestLogConfig
}

var sensitiveKeys = []string{
	"authorization",
	"cookie",
	"set-cookie",
}

// Bundle exposes the Keelith telemetry lifecycle and its protocol middleware
// as one immutable application policy result.
type Bundle struct {
	telemetry *keelithobs.Bundle
	server    *middleware.Bundle
	stream    *middleware.StreamBundle
}

// New constructs one App-scoped Keelith observability policy. It does not
// install global slog or OpenTelemetry providers.
func New(config Config) (*Bundle, error) {
	if config.Output == nil {
		return nil, fmt.Errorf("observability: output is required")
	}
	telemetry, err := keelithobs.New(keelithobs.Config{
		Resource: kresource.Config{
			ServiceName: config.ServiceName,
			Environment: config.Environment,
		},
		LogOutput:     config.Output,
		Logging:       &config.Logging,
		AuditHandler:  slog.NewJSONHandler(config.Output, nil),
		SensitiveKeys: append([]string(nil), sensitiveKeys...),
	})
	if err != nil {
		return nil, fmt.Errorf("observability: build Keelith telemetry: %w", err)
	}
	if err := config.RequestLogs.Validate(); err != nil {
		_ = telemetry.Shutdown(context.Background())
		return nil, fmt.Errorf("observability: validate request logs: %w", err)
	}
	serverObservability := telemetry.ServerMiddleware()
	streamEntries := []middleware.StreamEntry{{
		Name: "observability", Middleware: telemetry.ServerStreamMiddleware(),
	}}
	if config.RequestLogsEnabled {
		requestLogger, err := logging.NewRequestLogger(
			telemetry.Logger().Slog(),
			config.RequestLogs,
		)
		if err != nil {
			_ = telemetry.Shutdown(context.Background())
			return nil, fmt.Errorf("observability: build request logger: %w", err)
		}
		serverObservability = middleware.Chain(
			serverObservability,
			requestLogger.Middleware(),
		)
		streamEntries = append(streamEntries, middleware.StreamEntry{
			Name: "request-log", Middleware: requestLogger.StreamMiddleware(),
		})
	}
	server, err := middleware.NewServerBundle(middleware.ServerBundleConfig{
		Observability: serverObservability,
		RecoveryReporter: func(ctx context.Context, report middleware.PanicReport) {
			telemetry.Logger().ErrorContext(ctx, "recovered request panic", "panic_type", report.Type, "stack", string(report.Stack))
		},
	})
	if err != nil {
		_ = telemetry.Shutdown(context.Background())
		return nil, fmt.Errorf("observability: build server middleware: %w", err)
	}
	stream, err := middleware.NewStreamBundle(streamEntries...)
	if err != nil {
		_ = telemetry.Shutdown(context.Background())
		return nil, fmt.Errorf("observability: build stream middleware: %w", err)
	}
	return &Bundle{
		telemetry: telemetry,
		server:    server,
		stream:    stream,
	}, nil
}

// LoggerDependencies returns the explicit wiring inputs owned by this policy.
func (b *Bundle) LoggerDependencies() (logging.Dependencies, *audit.Logger, error) {
	if b == nil || b.telemetry == nil {
		return logging.Dependencies{}, nil, fmt.Errorf("observability: bundle is nil")
	}
	dependencies, err := logging.NewDependencies(
		b.telemetry.Logger(),
		b.telemetry.LoggingController(),
	)
	return dependencies, b.telemetry.AuditLogger(), err
}

// Telemetry returns the lifecycle-owned Keelith log, trace, and metric bundle.
func (b *Bundle) Telemetry() *keelithobs.Bundle {
	if b == nil {
		return nil
	}
	return b.telemetry
}

// ServerMiddleware returns the immutable unary server middleware policy.
func (b *Bundle) ServerMiddleware() *middleware.Bundle {
	if b == nil {
		return nil
	}
	return b.server
}

// StreamMiddleware returns the immutable stream server middleware policy.
func (b *Bundle) StreamMiddleware() *middleware.StreamBundle {
	if b == nil {
		return nil
	}
	return b.stream
}
