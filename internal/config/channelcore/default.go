package channelcore

import (
	"fmt"
	"strings"
	"time"

	sqlruntime "github.com/keelab/contrib/data/sql"
	outboxruntime "github.com/keelab/contrib/data/sql/outbox"
	redisruntime "github.com/keelab/contrib/redis"
	"github.com/keelab/keelith/observability/logging"
)

func Default() Config {
	return Config{
		AppName:         "xxx",
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
			Name:        "xxx",
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
		ClientName:         "xxx",
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
		Table:          "xxx_outbox",
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

func Validate(config Config) error {
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
	for index, channel := range config.Channels {
		if strings.TrimSpace(channel.ID) == "" || strings.TrimSpace(channel.ID) != channel.ID {
			return fmt.Errorf("runtime.channels[%d].id is invalid", index)
		}
		if strings.TrimSpace(channel.Kind) == "" || strings.TrimSpace(channel.Kind) != channel.Kind {
			return fmt.Errorf("runtime.channels[%d].kind is invalid", index)
		}
		if channel.Enabled && channel.Kind == "feishu" && (strings.TrimSpace(channel.AppID) == "" || strings.TrimSpace(channel.AppSecret) == "") {
			return fmt.Errorf("runtime.channels[%d] feishu app_id and app_secret are required", index)
		}
		if channel.Enabled {
			switch channel.Kind {
			case "feishu", "telegram", "qq", "dingtalk", "wecom", "wecom_app", "wecom_aibot", "webhook", "devops_publish":
			default:
				return fmt.Errorf("runtime.channels[%d] unsupported kind %q", index, channel.Kind)
			}
			switch channel.Kind {
			case "telegram":
				if strings.TrimSpace(channel.Telegram.Token) == "" {
					return fmt.Errorf("runtime.channels[%d] telegram token is required", index)
				}
			case "qq":
				if strings.TrimSpace(channel.QQ.AppID) == "" || strings.TrimSpace(channel.QQ.AppSecret) == "" {
					return fmt.Errorf("runtime.channels[%d] qq app_id and app_secret are required", index)
				}
			case "dingtalk":
				if strings.TrimSpace(channel.DingTalk.ClientID) == "" || strings.TrimSpace(channel.DingTalk.ClientSecret) == "" {
					return fmt.Errorf("runtime.channels[%d] dingtalk credentials are required", index)
				}
			case "wecom":
				if strings.TrimSpace(channel.WeCom.WebhookURL) == "" {
					return fmt.Errorf("runtime.channels[%d] wecom webhook_url is required", index)
				}
			case "wecom_app":
				if strings.TrimSpace(channel.WeComApp.CorpID) == "" || strings.TrimSpace(channel.WeComApp.CorpSecret) == "" || channel.WeComApp.AgentID == 0 {
					return fmt.Errorf("runtime.channels[%d] wecom_app credentials are required", index)
				}
			case "devops_publish":
				if strings.TrimSpace(channel.DevOpsPublish.ServerURL) == "" || strings.TrimSpace(channel.DevOpsPublish.AccountID) == "" || strings.TrimSpace(channel.DevOpsPublish.Credential) == "" {
					return fmt.Errorf("runtime.channels[%d] devops_publish credentials are required", index)
				}
			}
		}
		if channel.RatePerSecond < 0 || channel.Burst < 0 || channel.QueueSize < 0 || channel.MaxRetries < 0 {
			return fmt.Errorf("runtime.channels[%d] queue and retry values must not be negative", index)
		}
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
