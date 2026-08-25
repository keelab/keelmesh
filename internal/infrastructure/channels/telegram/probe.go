package telegram

import (
	"context"
	"errors"
)

func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return errors.New("channelcore: channel is disabled")
	}
	var result map[string]any
	return c.call(ctx, "getMe", nil, &result)
}
