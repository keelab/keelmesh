package webhook

import (
	"context"
	"errors"
	"fmt"
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
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("webhook: probe returned %s", resp.Status)
	}
	return nil
}
