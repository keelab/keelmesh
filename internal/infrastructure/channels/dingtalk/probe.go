package dingtalk

import (
	"context"
	"errors"
)

func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return errors.New("channelcore: channel is disabled")

	}
	if !c.Running() {
		return errors.New("dingtalk: stream is not running")
	}
	return ctx.Err()
}
