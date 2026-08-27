package observability

import (
	"fmt"
	"io"

	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelmesh/internal/config/channelcore"
)

func BuildObsPolicy(cfg channelcore.Config, output io.Writer) (*Bundle, error) {
	policy, err := New(Config{
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
	return policy, nil
}
