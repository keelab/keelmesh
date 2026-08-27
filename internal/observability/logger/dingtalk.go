package logger

import (
	"context"
	"log/slog"

	"github.com/keelab/keelith/observability/logging"
)

// DingTalkLogger adapts the DingTalk stream logger to Keelith's logger.
type DingTalkLogger struct {
	logger *logging.Logger
}

// NewDingTalk creates a new DingTalkLogger.
func NewDingTalk(logger *logging.Logger) *DingTalkLogger {
	return &DingTalkLogger{
		logger: logger,
	}
}

// Debugf logs a debug message.
func (l *DingTalkLogger) Debugf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "dingtalk", formatted(format, args...))
}

// Infof logs an info message.
func (l *DingTalkLogger) Infof(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "dingtalk", formatted(format, args...))
}

// Warningf logs a warning message.
func (l *DingTalkLogger) Warningf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "dingtalk", formatted(format, args...))
}

// Errorf logs an error message.
func (l *DingTalkLogger) Errorf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelError, "dingtalk", formatted(format, args...))
}

// Fatalf logs a fatal message.
func (l *DingTalkLogger) Fatalf(format string, args ...any) {
	logValue(l.logger, context.Background(), slog.LevelError, "dingtalk", "fatal: "+formatted(format, args...))
}
