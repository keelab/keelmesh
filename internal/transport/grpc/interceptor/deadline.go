package interceptor

import (
	"context"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// NewClientDeadlineBundle creates the gRPC interceptor chain that requires a
// client deadline.
func NewClientDeadlineBundle() (*middleware.Bundle, error) {
	return middleware.NewBundle(middleware.Entry{
		Name:       "grpc-client-deadline",
		Source:     "keelmesh",
		Middleware: RequireClientDeadline(),
	})
}

// RequireClientDeadline rejects gRPC calls without a client deadline.
// Requiring callers to declare a time budget prevents abandoned internal RPCs
// from consuming resources indefinitely.
func RequireClientDeadline() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			target, ok := operation.FromContext(ctx)
			if !ok || target.Transport() != "grpc" {
				return next(ctx, request)
			}
			if _, ok := ctx.Deadline(); !ok {
				return nil, kerrors.New(
					412,
					"CLIENT_DEADLINE_REQUIRED",
					"gRPC calls require a client deadline",
				)
			}
			return next(ctx, request)
		}
	}
}
