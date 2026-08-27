package channels

import (
	"fmt"

	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelmesh/internal/config/channelcore"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/dingtalk"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/feishu"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/qq"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/telegram"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/webhook"
	"github.com/keelab/keelmesh/internal/infrastructure/channels/wecom"
	"github.com/keelab/keelmesh/internal/observability/logger"
)

type adapterBuilder func(channelcore.Definition) (domain.Channel, error)

type adapterFactory struct {
	clients   *HTTPClients
	media     domain.MediaDomain
	sdkLogger *logging.Logger
	builders  map[string]adapterBuilder
}

func newAdapterFactory(clients *HTTPClients, media domain.MediaDomain, sdkLogger *logging.Logger) *adapterFactory {
	factory := &adapterFactory{
		clients:   clients,
		media:     media,
		sdkLogger: sdkLogger,
	}
	factory.builders = map[string]adapterBuilder{
		"feishu":      factory.buildFeishu,
		"telegram":    factory.buildTelegram,
		"webhook":     factory.buildWebhook,
		"dingtalk":    factory.buildDingTalk,
		"qq":          factory.buildQQ,
		"wecom":       factory.buildWeCom,
		"wecom_app":   factory.buildWeComApp,
		"wecom_aibot": factory.buildWeComAIBot,
	}
	return factory
}

func (f *adapterFactory) Build(definition channelcore.Definition) (domain.Channel, error) {
	builder, ok := f.builders[definition.Kind]
	if !ok {
		if definition.Enabled {
			return nil, fmt.Errorf("unsupported enabled channel kind %q", definition.Kind)
		}
		return nil, nil
	}
	return builder(definition)
}

func (f *adapterFactory) buildFeishu(definition channelcore.Definition) (domain.Channel, error) {
	return feishu.New(feishu.Config{
		ID: definition.ID, Enabled: definition.Enabled, AppID: definition.AppID, AppSecret: definition.AppSecret,
		EncryptKey: definition.EncryptKey, VerificationToken: definition.VerificationToken,
		AllowFrom: definition.AllowFrom, GroupTrigger: groupTrigger(definition.GroupTrigger),
		MediaRoot: definition.MediaRoot, MediaStore: f.media, HTTPClient: f.clients.Feishu,
		Logger: logger.NewFeishu(f.sdkLogger), RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildTelegram(definition channelcore.Definition) (domain.Channel, error) {
	return telegram.New(telegram.Config{
		ID: definition.ID, Enabled: definition.Enabled, Token: definition.Telegram.Token,
		BaseURL: definition.Telegram.BaseURL, Proxy: definition.Telegram.Proxy,
		AllowFrom: definition.Telegram.AllowFrom, GroupTrigger: groupTrigger(definition.Telegram.GroupTrigger),
		PlaceholderText: definition.Telegram.Placeholder.Text, MediaStore: f.media,
		HTTPClient: f.clients.Telegram, RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildWebhook(definition channelcore.Definition) (domain.Channel, error) {
	return webhook.New(webhook.Config{
		ID: definition.ID, Enabled: definition.Enabled, OutboundURL: definition.Webhook.OutboundURL,
		Listen: definition.Webhook.Listen, Path: definition.Webhook.Path, Secret: definition.Webhook.Secret,
		AllowFrom: definition.Webhook.AllowFrom, MediaStore: f.media, HTTPClient: f.clients.Webhook,
		RatePerSecond: definition.RatePerSecond, Burst: definition.Burst,
		QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildDingTalk(definition channelcore.Definition) (domain.Channel, error) {
	return dingtalk.New(dingtalk.Config{
		ID: definition.ID, Enabled: definition.Enabled, ClientID: definition.DingTalk.ClientID,
		ClientSecret: definition.DingTalk.ClientSecret, AllowFrom: definition.DingTalk.AllowFrom,
		GroupTrigger: groupTrigger(definition.DingTalk.GroupTrigger), RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildQQ(definition channelcore.Definition) (domain.Channel, error) {
	return qq.New(qq.Config{
		ID: definition.ID, Enabled: definition.Enabled, AppID: definition.QQ.AppID,
		AppSecret: definition.QQ.AppSecret, AllowFrom: definition.QQ.AllowFrom,
		GroupTrigger: groupTrigger(definition.QQ.GroupTrigger), RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildWeCom(definition channelcore.Definition) (domain.Channel, error) {
	return wecom.New(wecom.Config{
		ID: definition.ID, Kind: "wecom", Enabled: definition.Enabled, WebhookURL: definition.WeCom.WebhookURL,
		Token: definition.WeCom.Token, EncodingAESKey: definition.WeCom.EncodingAESKey,
		Listen: definition.WeCom.WebhookListen, Path: definition.WeCom.WebhookPath,
		AllowFrom: definition.WeCom.AllowFrom, GroupTrigger: groupTrigger(definition.WeCom.GroupTrigger),
		MediaStore: f.media, HTTPClient: f.clients.WeCom, RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildWeComApp(definition channelcore.Definition) (domain.Channel, error) {
	return wecom.New(wecom.Config{
		ID: definition.ID, Kind: "wecom_app", Enabled: definition.Enabled,
		CorpID: definition.WeComApp.CorpID, CorpSecret: definition.WeComApp.CorpSecret,
		AgentID: definition.WeComApp.AgentID, Token: definition.WeComApp.Token,
		EncodingAESKey: definition.WeComApp.EncodingAESKey,
		Listen:         definition.WeComApp.WebhookListen, Path: definition.WeComApp.WebhookPath,
		AllowFrom: definition.WeComApp.AllowFrom, GroupTrigger: groupTrigger(definition.WeComApp.GroupTrigger),
		MediaStore: f.media, HTTPClient: f.clients.WeCom, RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}

func (f *adapterFactory) buildWeComAIBot(definition channelcore.Definition) (domain.Channel, error) {
	return wecom.New(wecom.Config{
		ID: definition.ID, Kind: "wecom_aibot", Enabled: definition.Enabled,
		Token: definition.WeComAIBot.Token, EncodingAESKey: definition.WeComAIBot.EncodingAESKey,
		Listen: definition.WeComAIBot.WebhookListen, Path: definition.WeComAIBot.WebhookPath,
		AllowFrom: definition.WeComAIBot.AllowFrom, GroupTrigger: groupTrigger(definition.WeComAIBot.GroupTrigger),
		MediaStore: f.media, HTTPClient: f.clients.WeCom, RatePerSecond: definition.RatePerSecond,
		Burst: definition.Burst, QueueSize: definition.QueueSize, MaxRetries: definition.MaxRetries,
	})
}
