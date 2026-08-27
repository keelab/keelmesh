package channels

import (
	"context"
	"fmt"
	"strings"

	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelmesh/internal/config/channelcore"
	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/infrastructure/gateclient"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/channel"
	"github.com/keelab/keelmesh/internal/infrastructure/persistence/memory/media"
	"github.com/keelab/keelmesh/internal/observability/logger"
	dingtalklogger "github.com/open-dingtalk/dingtalk-stream-sdk-go/logger"
	"github.com/tencent-connect/botgo"
)

// Stack contains the channel domain and the handles needed by the application bootstrap.
type Stack struct {
	Domain   *channel.Repository
	Channels []domain.Channel
	Gate     *gateclient.Client
}

func Build(ctx context.Context, cfg channelcore.Config, clients *HTTPClients, sdkLogger *logging.Logger) (*Stack, error) {
	dingtalklogger.SetLogger(logger.NewDingTalk(sdkLogger))
	botgo.SetLogger(logger.NewQQ(sdkLogger))

	sharedMediaStore, err := mediaStoreFor(cfg.Channels)
	if err != nil {
		return nil, fmt.Errorf("build shared media store: %w", err)
	}

	registry := channel.NewRegistry()

	factory := newAdapterFactory(clients, sharedMediaStore, sdkLogger)
	for _, definition := range cfg.Channels {
		c, buildErr := factory.Build(definition)
		if buildErr != nil {
			return nil, fmt.Errorf("build channel %q: %w", definition.ID, buildErr)
		}
		if c == nil {
			continue
		}
		if err := registry.Register(c); err != nil {
			return nil, fmt.Errorf("register channel %q: %w", definition.ID, err)
		}
	}

	channels, err := channel.NewRepository(registry)
	if err != nil {
		return nil, fmt.Errorf("build channel runtime: %w", err)
	}
	if sharedMediaStore != nil {
		channels.SetMediaStore(sharedMediaStore)
	}

	var gate *gateclient.Client
	if strings.TrimSpace(cfg.GateCoreAddress) != "" {
		gate, err = gateclient.New(cfg.GateCoreAddress)
		if err != nil {
			_ = channels.Stop(context.WithoutCancel(ctx))
			return nil, fmt.Errorf("build GateCore client: %w", err)
		}
		channels.SetInboundForwarder(gate.IngestInbound)
		channels.SetOutboundAuthorizer(gate.AuthorizeOutbound)
	}

	return &Stack{
		Domain:   channels,
		Channels: registry.All(),
		Gate:     gate,
	}, nil
}

func groupTrigger(config channelcore.GroupTriggerConfig) domain.GroupTriggerPolicy {
	return domain.GroupTriggerPolicy{
		MentionOnly: config.MentionOnly,
		Prefixes:    append([]string(nil), config.Prefixes...),
	}
}

func mediaStoreFor(definitions []channelcore.Definition) (domain.MediaDomain, error) {
	root := ".data/media"
	for _, definition := range definitions {
		if strings.TrimSpace(definition.MediaRoot) == "" {
			continue
		}
		root = definition.MediaRoot
		break
	}
	store, err := media.NewRepository(root)
	if err != nil {
		return nil, fmt.Errorf("initialize media repository at %q: %w", root, err)
	}
	return store, nil
}
