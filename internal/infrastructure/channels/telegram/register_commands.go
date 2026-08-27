package telegram

import (
	"context"
	"fmt"
	"strings"

	"github.com/keelab/keelmesh/internal/domain"
)

// RegisterCommands publishes the command names and descriptions supported by
// Telegram. Telegram does not expose aliases or subcommands in this API, so
// those transport-neutral fields remain local to the command router.
func (c *Channel) RegisterCommands(ctx context.Context, definitions []domain.CommandDefinition) error {
	commands := make([]map[string]string, 0, len(definitions))
	for _, definition := range definitions {
		name := strings.TrimSpace(definition.Name)
		description := strings.TrimSpace(definition.Description)
		if name == "" || description == "" {
			return fmt.Errorf("telegram: command name and description are required")
		}
		commands = append(commands, map[string]string{
			"command":     name,
			"description": description,
		})
	}

	return c.call(ctx, "setMyCommands", map[string]any{"commands": commands}, nil)
}
