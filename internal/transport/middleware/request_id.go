package middleware

import (
	"context"

	"github.com/keelab/keelith/correlation"
	kmiddleware "github.com/keelab/keelith/middleware"
)

const requestIDKey = correlation.RequestIDMetadataKey

// RequestID returns the protocol-validated request identifier from context.
func RequestID(ctx context.Context) (string, bool) {
	return correlation.RequestID(ctx)
}

// NewRequestIDBundle creates a reusable chain that can be attached to any
// generated service Binding for either HTTP or gRPC.
func NewRequestIDBundle() (*kmiddleware.Bundle, error) {
	return kmiddleware.NewBundle(kmiddleware.Entry{
		Name:       "request-id",
		Source:     "demo-shared",
		Middleware: correlation.RequireRequestID(),
	})
}

// RequireRequestID rejects calls without a propagated request identifier.
func RequireRequestID() kmiddleware.Middleware {
	return correlation.RequireRequestID()
}
