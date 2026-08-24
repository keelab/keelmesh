package config

import (
	"fmt"
	"strings"
	"time"

	sqlruntime "github.com/keelab/contrib/data/sql"
	outboxruntime "github.com/keelab/contrib/data/sql/outbox"
	redisruntime "github.com/keelab/contrib/redis"
	"github.com/keelab/keelith/observability/logging"
)

func DefaultChannel() ChannelConfig {
	return ChannelConfig{
		AppName:         "channelcore",
		Environment:     "development",
		GRPCAddr:        "0.0.0.0:9010",
		GRPCOpsAddr:     "127.0.0.1:9011",
		ShutdownTimeout: 10 * time.Second,
		Logging: LoggingConfig{
			Level:  "info",
			Format: logging.FormatJSON,
		},
		Observability: ObservabilityConfig{
			RequestLogs: RequestLogsConfig{
				Enabled:            true,
				SuccessSampleEvery: 1,
				SlowThreshold:      500 * time.Millisecond,
			},
		},
		OpsHealthOnly:       true,
		DependenciesEnabled: false,
		SecretRoot:          ".secrets",
	}
}

func DefaultSQL() sqlruntime.ConnectionConfig {
	return sqlruntime.ConnectionConfig{
		Driver:       "pgx",
		DSNReference: "secret://file/postgres-dsn",
		Pool: sqlruntime.Config{
			Owns:        true,
			System:      "postgresql",
			Name:        "channelcore",
			MaxIdle:     4,
			MaxOpen:     16,
			MaxIdleTime: 5 * time.Minute,
			MaxLifetime: 30 * time.Minute,
		},
	}
}

func DefaultRedis() redisruntime.Config {
	return redisruntime.Config{
		Mode:               redisruntime.ModeStandalone,
		Addresses:          []string{"127.0.0.1:6379"},
		ClientName:         "channelcore",
		Protocol:           3,
		MaxRetries:         2,
		DialTimeout:        2 * time.Second,
		ReadTimeout:        time.Second,
		WriteTimeout:       time.Second,
		PoolTimeout:        2 * time.Second,
		PoolSize:           16,
		MinIdleConnections: 1,
		MaxIdleConnections: 8,
	}
}

func DefaultOutbox() outboxruntime.RuntimeConfig {
	return outboxruntime.RuntimeConfig{
		Table:          "channelcore_outbox",
		Isolation:      "read-committed",
		PollInterval:   250 * time.Millisecond,
		ErrorDelay:     time.Second,
		LeaseTTL:       30 * time.Second,
		PublishTimeout: 10 * time.Second,
		BatchSize:      100,
		MaxAttempts:    20,
		RetryBase:      time.Second,
		RetryMax:       time.Minute,
	}
}

func ValidateChannel(config ChannelConfig) error {
	for name, value := range map[string]string{
		"appName":        config.AppName,
		"environment":    config.Environment,
		"grpcAddress":    config.GRPCAddr,
		"grpcOpsAddress": config.GRPCOpsAddr,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return fmt.Errorf("runtime.%s must not be empty or padded", name)
		}
	}
	if config.ShutdownTimeout <= 0 {
		return fmt.Errorf("runtime.shutdownTimeout must be greater than zero")
	}
	if _, err := logging.ParseLevel(config.Logging.Level); err != nil {
		return fmt.Errorf("runtime.logging.level: %w", err)
	}
	switch config.Logging.Format {
	case logging.FormatJSON, logging.FormatText:
	default:
		return fmt.Errorf("runtime.logging.format must be json or text")
	}
	if err := config.Observability.RequestLogs.Policy().Validate(); err != nil {
		return fmt.Errorf("runtime.observability.requestLogs: %w", err)
	}
	if config.DependenciesEnabled && strings.TrimSpace(config.SecretRoot) == "" {
		return fmt.Errorf("runtime.secretRoot is required when dependencies are enabled")
	}
	return nil
}
