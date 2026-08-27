package channel

import (
	"context"
	"errors"
)

func retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return !errors.Is(err, ErrInvalidMessage) &&
		!errors.Is(err, ErrChannelDisabled) &&
		!errors.Is(err, ErrUnsupported) &&
		!errors.Is(err, ErrQueueFull)
}
