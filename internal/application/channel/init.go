package channel

import (
	"context"

	"github.com/keelab/keelith/observability/logging"
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/domain"
)

type Service struct {
	channelv1.UnimplementedChannelServiceKeelithServer
	runtime domain.ChannelDomain
	logger  *logging.Logger
}

func New(runtime domain.ChannelDomain, logger *logging.Logger) *Service {
	return &Service{
		runtime: runtime,
		logger:  logger.WithCallerSkip(1),
	}
}

func (s *Service) logError(ctx context.Context, operation string, err error, args ...any) {
	if s.logger == nil || err == nil {
		return
	}
	logArgs := make([]any, 0, len(args)+4)
	logArgs = append(logArgs, "operation", operation, "error", err)
	logArgs = append(logArgs, args...)
	s.logger.ErrorContext(ctx, "channel operation failed", logArgs...)
}
