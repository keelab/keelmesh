package channel

import (
	"context"
	"fmt"

	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

// RegisterCommands registers platform-native command definitions.
func (s *Service) RegisterCommands(ctx context.Context, request *channelv1.RegisterCommandsRequest) (*channelv1.RegisterCommandsResponse, error) {
	candidate, err := s.runtime.Get(request.GetChannelId())
	if err != nil {
		return nil, err
	}
	registrar, ok := candidate.(domain.CommandRegistrar)
	if !ok {
		return &channelv1.RegisterCommandsResponse{
			ChannelId: request.GetChannelId(),
			State:     "unsupported",
			Reason:    "channel does not support native command registration",
		}, nil
	}

	commands := make([]domain.CommandDefinition, 0, len(request.GetCommands()))
	for _, command := range request.GetCommands() {
		if command == nil {
			return nil, fmt.Errorf("channelcore: command definition is nil")
		}
		definition := domain.CommandDefinition{
			Name:        command.GetName(),
			Description: command.GetDescription(),
			Usage:       command.GetUsage(),
			Aliases:     append([]string(nil), command.GetAliases()...),
			SubCommands: make([]domain.CommandSubcommand, 0, len(command.GetSubcommands())),
		}
		for _, subcommand := range command.GetSubcommands() {
			if subcommand == nil {
				return nil, fmt.Errorf("channelcore: command %q has nil subcommand", command.GetName())
			}
			definition.SubCommands = append(definition.SubCommands, domain.CommandSubcommand{
				Name:        subcommand.GetName(),
				Description: subcommand.GetDescription(),
				ArgsUsage:   subcommand.GetArgsUsage(),
			})
		}
		commands = append(commands, definition)
	}
	if err := registrar.RegisterCommands(ctx, commands); err != nil {
		s.logError(ctx, "register_commands", err, "channel_id", request.GetChannelId())
		return nil, err
	}

	return &channelv1.RegisterCommandsResponse{
		ChannelId:    request.GetChannelId(),
		Registered:   true,
		CommandCount: int32(len(commands)),
		State:        "registered",
	}, nil
}
