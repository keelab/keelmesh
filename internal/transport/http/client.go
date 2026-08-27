package http

import (
	"context"
	stdhttp "net/http"

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
func New(standard *stdhttp.Client, bundle *middleware.Bundle, policy metadata.Policy, propagator propagation.TextMapPropagator, maxResponseBytes int64) (*Client, error) {
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

// Do invokes one external HTTP operation and returns the decoder's concrete type.
func Do[T any](ctx context.Context, c *Client, service string, method string, request *stdhttp.Request, decode func(context.Context, *stdhttp.Response) (T, error)) (T, error) {
	var zero T
	target, err := operation.New("http", service, method, operation.KindUnary)
	if err != nil {
		return zero, err
	}

	return khttp.Invoke(ctx, c.inner, target, khttp.ClientCall[T]{
		Request: request,
		Decode:  decode,
	})
}
