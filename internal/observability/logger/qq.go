package logger

import (
	"context"
	"log/slog"

	"github.com/keelab/keelith/observability/logging"
)

// QQLogger adapts the BotGo logger to Keelith's logger.
type QQLogger struct {
	logger *logging.Logger
}

func NewQQ(logger *logging.Logger) *QQLogger {
	return &QQLogger{
		logger: logger,
	}
}

func (l *QQLogger) Debug(args ...any) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "qq", message(args...))
}

func (l *QQLogger) Info(args ...any) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "qq", message(args...))
}

func (l *QQLogger) Warn(args ...any) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "qq", message(args...))
}

func (l *QQLogger) Error(args ...any) {
	logValue(l.logger, context.Background(), slog.LevelError, "qq", message(args...))
}

func (l *QQLogger) Debugf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "qq", formatted(format, args...))
}

func (l *QQLogger) Infof(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "qq", formatted(format, args...))
}

func (l *QQLogger) Warnf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "qq", formatted(format, args...))
}

func (l *QQLogger) Errorf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelError, "qq", formatted(format, args...))
}

func (l *QQLogger) Sync() error { return nil }
