package webhook

import (
	"context"
	"errors"
	stdhttp "net/http"

	"github.com/keelab/keelmesh/internal/transport/http"
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
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodHead, c.config.OutboundURL, nil)
	if err != nil {
		return err
	}
	_, err = http.Do[any](ctx, c.client, "webhook", "probe", req, func(_ context.Context, response *stdhttp.Response) (any, error) {
		return nil, nil
	})
	return err
}
