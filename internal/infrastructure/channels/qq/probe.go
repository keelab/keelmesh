package qq

import (
	"context"
	"errors"
)

func (c *Channel) Probe(context.Context) error {
	if !c.config.Enabled {
		return errors.New("channelcore: channel is disabled")
	}
	if !c.Running() {
		return errors.New("qq: websocket is not running")
	}
	return nil
}
