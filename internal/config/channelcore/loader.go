package channelcore

import (
	"context"
	"fmt"

	ksql "github.com/keelab/contrib/data/sql"
	koutbox "github.com/keelab/contrib/data/sql/outbox"
	kredis "github.com/keelab/contrib/redis"
	kconfig "github.com/keelab/keelith/config"
	kenv "github.com/keelab/keelith/config/env"
	configfile "github.com/keelab/keelith/config/file"
)

func Loade(ctx context.Context, path string) (Loaded, error) {
	fileSource, err := configfile.New(path, configfile.WithMaxBytes(1<<20))
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create file source: %w", err)
	}
	environmentSource, err := kenv.New(
		"KEELMESH_CHANNELCORE_",
		kenv.WithParser(kenv.JSONValueParser),
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create environment source: %w", err)
	}
	runtimeBinding, err := kconfig.NewComponent[Config](
		"channelcore-runtime",
		"runtime",
		kconfig.WithComponentDefault(Default()),
		kconfig.WithComponentValidator(Validate),
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create runtime binding: %w", err)
	}
	sqlBinding, err := ksql.NewConnectionConfigBinding(
		"component.sql.primary",
		"components.sql.primary",
		kconfig.WithComponentDefault(DefaultSQL()),
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create SQL binding: %w", err)
	}
	redisBinding, err := kredis.NewConfigBinding(
		"component.redis.cache",
		"components.redis.cache",
		kconfig.WithComponentDefault(DefaultRedis()),
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create Redis binding: %w", err)
	}
	outboxBinding, err := koutbox.NewRuntimeConfigBinding(
		"component.outbox.delivery",
		"components.outbox.delivery",
		kconfig.WithComponentDefault(DefaultOutbox()),
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create Outbox binding: %w", err)
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
		return Loaded{}, fmt.Errorf("config: create manager: %w", err)
	}
	if _, err := manager.Load(ctx); err != nil {
		return Loaded{}, fmt.Errorf("config: initial load: %w", err)
	}
	runtimeConfig, ok := runtimeBinding.Current()
	if !ok {
		return Loaded{}, fmt.Errorf("config: runtime snapshot was not published")
	}
	runtimeServer, err := kconfig.NewRuntime(manager)
	if err != nil {
		return Loaded{}, fmt.Errorf("config: create watch runtime: %w", err)
	}

	return Loaded{
		Runtime:       runtimeConfig,
		Manager:       manager,
		RuntimeServer: runtimeServer,
		SQL:           sqlBinding,
		Redis:         redisBinding,
		Outbox:        outboxBinding,
	}, nil
}
