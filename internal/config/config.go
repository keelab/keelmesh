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
	Channels            []ChannelDefinition `config:"channels"`
}

type ChannelDefinition struct {
	ID                string                     `config:"id"`
	Kind              string                     `config:"kind"`
	Enabled           bool                       `config:"enabled"`
	AppID             string                     `config:"app_id"`
	AppSecret         string                     `config:"app_secret"`
	EncryptKey        string                     `config:"encrypt_key"`
	VerificationToken string                     `config:"verification_token"`
	AllowFrom         []string                   `config:"allow_from"`
	MediaRoot         string                     `config:"media_root"`
	RatePerSecond     float64                    `config:"rate_per_second"`
	Burst             int                        `config:"burst"`
	QueueSize         int                        `config:"queue_size"`
	MaxRetries        int                        `config:"max_retries"`
	Telegram          TelegramChannelConfig      `config:"telegram"`
	QQ                QQChannelConfig            `config:"qq"`
	DingTalk          DingTalkChannelConfig      `config:"dingtalk"`
	WeCom             WeComChannelConfig         `config:"wecom"`
	WeComApp          WeComAppChannelConfig      `config:"wecom_app"`
	WeComAIBot        WeComAIBotChannelConfig    `config:"wecom_aibot"`
	Webhook           WebhookChannelConfig       `config:"webhook"`
	DevOpsPublish     DevOpsPublishChannelConfig `config:"devops_publish"`
}

type GroupTriggerConfig struct {
	MentionOnly bool     `config:"mention_only"`
	Prefixes    []string `config:"prefixes"`
}

type TypingConfig struct {
	Enabled bool `config:"enabled"`
}
type PlaceholderConfig struct {
	Enabled bool   `config:"enabled"`
	Text    string `config:"text"`
}

type TelegramChannelConfig struct {
	Token        string             `config:"token"`
	BaseURL      string             `config:"base_url"`
	Proxy        string             `config:"proxy"`
	AllowFrom    []string           `config:"allow_from"`
	GroupTrigger GroupTriggerConfig `config:"group_trigger"`
	Typing       TypingConfig       `config:"typing"`
	Placeholder  PlaceholderConfig  `config:"placeholder"`
}

type QQChannelConfig struct {
	AppID         string   `config:"app_id"`
	AppSecret     string   `config:"app_secret"`
	AllowFrom     []string `config:"allow_from"`
	WebhookListen string   `config:"webhook_listen"`
	WebhookPath   string   `config:"webhook_path"`
}
type DingTalkChannelConfig struct {
	ClientID     string   `config:"client_id"`
	ClientSecret string   `config:"client_secret"`
	AllowFrom    []string `config:"allow_from"`
}
type WeComChannelConfig struct {
	Token          string   `config:"token"`
	EncodingAESKey string   `config:"encoding_aes_key"`
	WebhookURL     string   `config:"webhook_url"`
	WebhookListen  string   `config:"webhook_listen"`
	WebhookPath    string   `config:"webhook_path"`
	AllowFrom      []string `config:"allow_from"`
}
type WeComAppChannelConfig struct {
	CorpID         string   `config:"corp_id"`
	CorpSecret     string   `config:"corp_secret"`
	AgentID        int64    `config:"agent_id"`
	Token          string   `config:"token"`
	EncodingAESKey string   `config:"encoding_aes_key"`
	WebhookListen  string   `config:"webhook_listen"`
	WebhookPath    string   `config:"webhook_path"`
	AllowFrom      []string `config:"allow_from"`
}
type WeComAIBotChannelConfig struct {
	Token          string   `config:"token"`
	EncodingAESKey string   `config:"encoding_aes_key"`
	WebhookListen  string   `config:"webhook_listen"`
	WebhookPath    string   `config:"webhook_path"`
	AllowFrom      []string `config:"allow_from"`
}
type WebhookChannelConfig struct {
	OutboundURL string   `config:"outbound_url"`
	Listen      string   `config:"listen"`
	Path        string   `config:"path"`
	Secret      string   `config:"secret"`
	AllowFrom   []string `config:"allow_from"`
}
type DevOpsPublishChannelConfig struct {
	ServerURL    string        `config:"server_url"`
	AccountID    string        `config:"account_id"`
	Credential   string        `config:"credential"`
	AllowFrom    []string      `config:"allow_from"`
	ReconnectMin time.Duration `config:"reconnect_min"`
	ReconnectMax time.Duration `config:"reconnect_max"`
	AckTimeout   time.Duration `config:"ack_timeout"`
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
