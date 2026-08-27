package dingtalk

import (
	"github.com/keelab/keelmesh/internal/domain"
)

func (c *Channel) Definition() domain.DefinitionEntity {
	return domain.DefinitionEntity{ID: c.config.ID, Kind: "dingtalk", Enabled: c.config.Enabled, Capabilities: []string{"messages", "inbound_stream", "groups", "session_reply"}, GroupTrigger: c.config.GroupTrigger, RatePerSecond: c.config.RatePerSecond, Burst: c.config.Burst, QueueSize: c.config.QueueSize, MaxRetries: c.config.MaxRetries}
}
