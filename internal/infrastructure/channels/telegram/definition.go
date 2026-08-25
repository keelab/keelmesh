package telegram

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Definition() domain.DefinitionEntity {
	return domain.DefinitionEntity{ID: c.config.ID, Kind: "telegram", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "media", "edit", "typing", "placeholder"}, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
