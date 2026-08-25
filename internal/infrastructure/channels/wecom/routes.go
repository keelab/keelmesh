package wecom

import (
	"net/http"

	khttp "github.com/keelab/keelith/transport/http"
	channelhttp "github.com/keelab/keelmesh/internal/transport/http"
)

// RegisterHTTP contributes this channel's callbacks to channelcore's shared router.
func (c *Channel) RegisterHTTP(router *khttp.Router) error {
	if !c.config.Enabled || c.config.Listen == "" {
		return nil
	}
	if err := channelhttp.Register(router, http.MethodGet, c.config.Path, "wecom-verify-"+c.config.ID, http.HandlerFunc(c.serve)); err != nil {
		return err
	}
	if err := channelhttp.Register(router, http.MethodPost, c.config.Path, "wecom-callback-"+c.config.ID, http.HandlerFunc(c.serve)); err != nil {
		return err
	}
	return channelhttp.Register(router, http.MethodGet, c.config.Path+"/health", "wecom-health-"+c.config.ID, http.HandlerFunc(c.health))
}
