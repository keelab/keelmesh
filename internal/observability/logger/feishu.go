package logger

import (
	"context"
	"log/slog"

	"github.com/keelab/keelith/observability/logging"
)

// FeishuLogger adapts larkcore.Logger to Keelith's structured logger.
type FeishuLogger struct {
	logger *logging.Logger
}

// NewFeishu creates a new FeishuLogger.
func NewFeishu(logger *logging.Logger) *FeishuLogger {
	return &FeishuLogger{
		logger: logger,
	}
}

// Debug logs a debug message.
func (l *FeishuLogger) Debug(ctx context.Context, args ...any) {
	logValue(l.logger, ctx, slog.LevelDebug, "feishu", message(args...))
}

// Info logs an info message.
func (l *FeishuLogger) Info(ctx context.Context, args ...any) {
	logValue(l.logger, ctx, slog.LevelInfo, "feishu", message(args...))
}

// Warn logs a warning message.
func (l *FeishuLogger) Warn(ctx context.Context, args ...any) {
	logValue(l.logger, ctx, slog.LevelWarn, "feishu", message(args...))
}

// Error logs an error message.
func (l *FeishuLogger) Error(ctx context.Context, args ...any) {
	logValue(l.logger, ctx, slog.LevelError, "feishu", message(args...))
}
