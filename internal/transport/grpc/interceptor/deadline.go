package interceptor

import (
	"context"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

// NewTaskServiceBundle creates the gRPC-only TaskService interceptor chain.
func NewTaskServiceBundle() (*middleware.Bundle, error) {
	return middleware.NewBundle(middleware.Entry{
		Name:       "grpc-task-client-deadline",
		Source:     "demo-grpc",
		Middleware: RequireTaskServiceDeadline(),
	})
}

// RequireTaskServiceDeadline rejects TaskService calls without a client
// deadline. Requiring callers to declare a time budget prevents abandoned
// internal RPCs from consuming resources indefinitely.
func RequireTaskServiceDeadline() middleware.Middleware {
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
					"gRPC TaskService calls require a client deadline",
				)
			}
			return next(ctx, request)
		}
	}
}
