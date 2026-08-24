package middleware

import (
	"context"
	"strings"

	kerrors "github.com/keelab/keelith/errors"
	"github.com/keelab/keelith/metadata"
	kmiddleware "github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
)

const idempotencyHeader = "x-idempotency-key"

// NewTaskServiceBundle creates the HTTP-only TaskService policy chain.
func NewTaskServiceBundle() (*kmiddleware.Bundle, error) {
	return kmiddleware.NewBundle(kmiddleware.Entry{
		Name:       "http-task-idempotency-key",
		Source:     "demo-http",
		Middleware: RequireTaskWriteIdempotencyKey(),
	})
}

// RequireTaskWriteIdempotencyKey rejects TaskService write operations that do
// not carry a non-empty x-idempotency-key header. The key is validated here;
// durable duplicate-result storage belongs in the application/infrastructure
// layers of a production service.
func RequireTaskWriteIdempotencyKey() kmiddleware.Middleware {
	return func(next kmiddleware.Handler) kmiddleware.Handler {
		return func(ctx context.Context, request any) (any, error) {
			target, ok := operation.FromContext(ctx)
			if !ok || target.Transport() != "http" || !isTaskWrite(target.Method()) {
				return next(ctx, request)
			}
			inbound, ok := metadata.Inbound(ctx)
			if !ok || !hasNonEmptyValue(inbound.Values(idempotencyHeader)) {
				return nil, kerrors.New(
					400,
					"IDEMPOTENCY_KEY_REQUIRED",
					"x-idempotency-key header is required for task writes",
				)
			}
			return next(ctx, request)
		}
	}
}

func isTaskWrite(method string) bool {
	return method == "CreateTask" || method == "CompleteTask"
}

func hasNonEmptyValue(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}
