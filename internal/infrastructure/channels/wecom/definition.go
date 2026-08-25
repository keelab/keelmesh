package wecom

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Definition() domain.DefinitionEntity {
	return domain.DefinitionEntity{
		ID:            c.config.ID,
		Kind:          c.config.Kind,
		Enabled:       c.config.Enabled,
		Capabilities:  []string{"messages", "inbound_stream", "webhook"},
		RatePerSecond: c.config.RatePerSecond,
		Burst:         c.config.Burst,
		QueueSize:     c.config.QueueSize,
		MaxRetries:    c.config.MaxRetries,
	}
}
