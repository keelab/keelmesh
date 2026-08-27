package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"
	"time"

	"github.com/keelab/keelmesh/internal/transport/http"
)

type tokenResponse struct {
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

func (c *Channel) Probe(ctx context.Context) error {
	if !c.config.Enabled {
		return errors.New("channelcore: channel is disabled")
	}
	if c.config.Kind == "wecom_app" {
		_, err := c.getToken(ctx)
		return err
	}
	if !c.Running() {
		return errors.New("wecom: listener is not running")
	}
	return nil
}

func (c *Channel) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.accessToken != "" && time.Now().Before(c.tokenExpire) {
		token := c.accessToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()
	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=" + url.QueryEscape(c.config.CorpID) + "&corpsecret=" + url.QueryEscape(c.config.CorpSecret)
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	result, err := http.Do[tokenResponse](ctx, c.client, "wecom", "getToken", req, func(_ context.Context, response *stdhttp.Response) (tokenResponse, error) {
		var result tokenResponse
		if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
			return tokenResponse{}, err
		}
		return result, nil
	})
	if err != nil {
		return "", err
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return "", fmt.Errorf("wecom: gettoken failed: %s", result.ErrMsg)
	}
	c.mu.Lock()
	c.accessToken = result.AccessToken
	c.tokenExpire = time.Now().Add(time.Duration(result.ExpiresIn-60) * time.Second)
	c.mu.Unlock()
	return result.AccessToken, nil
}
