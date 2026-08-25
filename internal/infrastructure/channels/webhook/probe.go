package webhook

import (
	"context"
	"errors"
	"net/http"
)

func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return errors.New("channelcore: channel is disabled")
	}
	if c.config.OutboundURL == "" {
		if c.Running() {
			return nil
		}
		return errors.New("webhook: listener is not running")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.config.OutboundURL, nil)
	if err != nil {
		return err
	}
	_, err = c.client.Do(ctx, "webhook", "probe", req, func(_ context.Context, response *http.Response) (any, error) {
		return nil, nil
	})
	return err
}
