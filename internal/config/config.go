package config

import (
	"time"

	sqlruntime "github.com/keelab/contrib/data/sql"
	outboxruntime "github.com/keelab/contrib/data/sql/outbox"
	redisruntime "github.com/keelab/contrib/redis"
	kconfig "github.com/keelab/keelith/config"
	"github.com/keelab/keelith/observability/logging"
)

// LoggingConfig is the application projection of Keelith's logging policy.
type LoggingConfig struct {
	Level     string         `config:"level"`
	Format    logging.Format `config:"format"`
	AddSource bool           `config:"add_source"`
}

// ObservabilityConfig contains always-on runtime signal policies.
type ObservabilityConfig struct {
	RequestLogs RequestLogsConfig `config:"request_logs"`
}

// RequestLogsConfig is the typed runtime projection for completion logs.
type RequestLogsConfig struct {
	Enabled            bool          `config:"enabled"`
	SuccessSampleEvery uint64        `config:"success_sample_every"`
	SlowThreshold      time.Duration `config:"slow_threshold"`
	DisableSlowLogs    bool          `config:"disable_slow_logs"`
}

// Policy returns the framework request logging policy.
func (c RequestLogsConfig) Policy() logging.RequestLogConfig {
	return logging.RequestLogConfig{
		SuccessSampleEvery: c.SuccessSampleEvery,
		SlowThreshold:      c.SlowThreshold,
		DisableSlowLogs:    c.DisableSlowLogs,
	}
}

// ChannelConfig contains process identity, listeners, shutdown, and dependency mode.
// Every field is validated and published atomically by Keelith config.Manager.
type ChannelConfig struct {
	AppName             string              `config:"app_name"`
	Environment         string              `config:"environment"`
	GRPCAddr            string              `config:"grpc_address"`
	GRPCOpsAddr         string              `config:"grpc_ops_address"`
	ShutdownTimeout     time.Duration       `config:"shutdown_timeout"`
	Logging             LoggingConfig       `config:"logging"`
	Observability       ObservabilityConfig `config:"observability"`
	OpsHealthOnly       bool                `config:"ops_health_only"`
	DependenciesEnabled bool                `config:"dependencies_enabled"`
	SecretRoot          string              `config:"secret_root"`
}

// ChannelLoaded is the initial typed snapshot plus its managed watch runtime.
type ChannelLoaded struct {
	Runtime       ChannelConfig
	Manager       *kconfig.Manager
	RuntimeServer *kconfig.Runtime
	SQL           *kconfig.Component[sqlruntime.ConnectionConfig]
	Redis         *kconfig.Component[redisruntime.Config]
	Outbox        *kconfig.Component[outboxruntime.RuntimeConfig]
}
