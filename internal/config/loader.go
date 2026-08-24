package config

import (
	"context"
	"fmt"

	sqlruntime "github.com/keelab/contrib/data/sql"
	outboxruntime "github.com/keelab/contrib/data/sql/outbox"
	redisruntime "github.com/keelab/contrib/redis"
	kconfig "github.com/keelab/keelith/config"
	configenv "github.com/keelab/keelith/config/env"
	configfile "github.com/keelab/keelith/config/file"
)

func LoadChannel(ctx context.Context, path string) (ChannelLoaded, error) {
	fileSource, err := configfile.New(path, configfile.WithMaxBytes(1<<20))
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create file source: %w", err)
	}
	environmentSource, err := configenv.New(
		"KEELMESH_CHANNELCORE_",
		configenv.WithParser(configenv.JSONValueParser),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create environment source: %w", err)
	}
	runtimeBinding, err := kconfig.NewComponent[ChannelConfig](
		"channelcore-runtime",
		"runtime",
		kconfig.WithComponentDefault(DefaultChannel()),
		kconfig.WithComponentValidator(ValidateChannel),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create runtime binding: %w", err)
	}
	sqlBinding, err := sqlruntime.NewConnectionConfigBinding(
		"component.sql.primary",
		"components.sql.primary",
		kconfig.WithComponentDefault(DefaultSQL()),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create SQL binding: %w", err)
	}
	redisBinding, err := redisruntime.NewConfigBinding(
		"component.redis.cache",
		"components.redis.cache",
		kconfig.WithComponentDefault(DefaultRedis()),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create Redis binding: %w", err)
	}
	outboxBinding, err := outboxruntime.NewRuntimeConfigBinding(
		"component.outbox.delivery",
		"components.outbox.delivery",
		kconfig.WithComponentDefault(DefaultOutbox()),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create Outbox binding: %w", err)
	}
	manager, err := kconfig.New(
		kconfig.WithSources(fileSource, environmentSource),
		kconfig.WithBindings(
			runtimeBinding,
			sqlBinding,
			redisBinding,
			outboxBinding,
		),
		kconfig.WithUnknownFieldPolicy(kconfig.UnknownReject),
		kconfig.WithKnownFields(
			"runtime.*",
			"components.sql.primary.*",
			"components.redis.cache.*",
			"components.outbox.delivery.*",
		),
	)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create manager: %w", err)
	}
	if _, err := manager.Load(ctx); err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: initial load: %w", err)
	}
	runtimeConfig, ok := runtimeBinding.Current()
	if !ok {
		return ChannelLoaded{}, fmt.Errorf("config: runtime snapshot was not published")
	}
	runtimeServer, err := kconfig.NewRuntime(manager)
	if err != nil {
		return ChannelLoaded{}, fmt.Errorf("config: create watch runtime: %w", err)
	}

	return ChannelLoaded{
		Runtime:       runtimeConfig,
		Manager:       manager,
		RuntimeServer: runtimeServer,
		SQL:           sqlBinding,
		Redis:         redisBinding,
		Outbox:        outboxBinding,
	}, nil
}
