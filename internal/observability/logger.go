package observability

import (
	"context"
	"fmt"

	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	"github.com/keelab/keelmesh/internal/config/channelcore"
)

// WireLogging installs the process logger, subscribes logging level to config,
// and returns DI-ready logging dependencies plus the audit logger.
func WireLogging(ctx context.Context, loaded channelcore.Loaded, policy *Bundle) (*logging.Dependencies, *audit.Logger, error) {
	logDeps, auditLog, err := policy.LoggerDependencies()
	if err != nil {
		_ = policy.telemetry.Shutdown(context.WithoutCancel(ctx))
		return nil, nil, fmt.Errorf("build logging dependencies: %w", err)
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
		_, applyErr := logDeps.Controller.ApplyBaseline(level)
		return applyErr
	}); err != nil {
		_ = policy.telemetry.Shutdown(context.WithoutCancel(ctx))
		return nil, nil, fmt.Errorf("subscribe logging level: %w", err)
	}

	return &logDeps, auditLog, nil
}
