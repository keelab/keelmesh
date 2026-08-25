package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	khttp "github.com/keelab/keelith/transport/http"
	"go.opentelemetry.io/otel/propagation"
)

// Client adapts Keelith's HTTP invocation policy to external protocol clients.
// Protocol adapters remain responsible for encoding and decoding vendor payloads.
type Client struct {
	inner *khttp.Client
}

// New builds an outbound HTTP client with Keelith transport policies.
func New(standard *nethttp.Client, bundle *middleware.Bundle, policy metadata.Policy, propagator propagation.TextMapPropagator, maxResponseBytes int64) (*Client, error) {
	options := make([]khttp.ClientOption, 0, 4)
	if bundle != nil {
		options = append(options, khttp.WithClientMiddleware(bundle))
	}
	options = append(options, khttp.WithClientMetadataPolicy(policy))
	if propagator != nil {
		options = append(options, khttp.WithClientPropagator(propagator))
	}
	if maxResponseBytes > 0 {
		options = append(options, khttp.WithClientMaxResponseBytes(maxResponseBytes))
	}
	client, err := khttp.NewClient(standard, options...)
	if err != nil {
		return nil, err
	}
	return &Client{inner: client}, nil
}

// Do invokes one external HTTP operation while preserving the protocol-specific decoder.
func (c *Client) Do(ctx context.Context, service string, method string, request *nethttp.Request, decode func(context.Context, *nethttp.Response) (any, error)) (any, error) {
	target, err := operation.New("http", service, method, operation.KindUnary)
	if err != nil {
		return nil, err
	}
	return c.inner.Invoke(ctx, target, khttp.ClientCall{
		Request: request,
		Decode:  decode,
	})
}

// DoRaw adapts Keelith transport governance to SDKs that accept net/http clients.
// The response body is buffered so the Keelith client can finish its lifecycle
// bookkeeping before ownership is handed back to the SDK.
func (c *Client) DoRaw(ctx context.Context, service string, method string, request *nethttp.Request) (*nethttp.Response, error) {
	value, err := c.Do(ctx, service, method, request, func(_ context.Context, response *nethttp.Response) (any, error) {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		cloned := new(nethttp.Response)
		*cloned = *response
		cloned.Body = io.NopCloser(bytes.NewReader(body))
		cloned.ContentLength = int64(len(body))
		return cloned, nil
	})
	if err != nil {
		return nil, err
	}
	response, ok := value.(*nethttp.Response)
	if !ok {
		return nil, fmt.Errorf("http: unexpected raw response type %T", value)
	}
	return response, nil
}
