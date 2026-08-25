package sdklog

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
)

var sensitiveQuery = regexp.MustCompile(`(?i)([?&](?:access[_-]?key|access[_-]?token|client[_-]?secret|ticket|token|secret|password|authorization|corp[_-]?secret|app[_-]?secret)=)[^&\s"']+`)
var sensitiveField = regexp.MustCompile(`(?i)(["']?(?:access[_-]?key|access[_-]?token|client[_-]?secret|ticket|token|secret|password|authorization|corp[_-]?secret|app[_-]?secret)["']?\s*[:=]\s*["']?)[^,\s&}"']+`)

func sanitize(value string) string {
	value = sensitiveQuery.ReplaceAllString(value, `${1}[REDACTED]`)
	return sensitiveField.ReplaceAllString(value, `${1}[REDACTED]`)
}

func message(args ...interface{}) string {
	return sanitize(fmt.Sprint(args...))
}

func formatted(format string, args ...interface{}) string {
	return sanitize(fmt.Sprintf(format, args...))
}

func logValue(logger *slog.Logger, ctx context.Context, level slog.Level, sdk, value string) {
	if logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	logger.Log(ctx, level, "sdk log", "sdk", sdk, "message", value)
}

// FeishuLogger adapts larkcore.Logger to Keelith's structured logger.
type FeishuLogger struct{ logger *slog.Logger }

func NewFeishu(logger *slog.Logger) *FeishuLogger { return &FeishuLogger{logger: logger} }
func (l *FeishuLogger) Debug(ctx context.Context, args ...interface{}) {
	logValue(l.logger, ctx, slog.LevelDebug, "feishu", message(args...))
}
func (l *FeishuLogger) Info(ctx context.Context, args ...interface{}) {
	logValue(l.logger, ctx, slog.LevelInfo, "feishu", message(args...))
}
func (l *FeishuLogger) Warn(ctx context.Context, args ...interface{}) {
	logValue(l.logger, ctx, slog.LevelWarn, "feishu", message(args...))
}
func (l *FeishuLogger) Error(ctx context.Context, args ...interface{}) {
	logValue(l.logger, ctx, slog.LevelError, "feishu", message(args...))
}

// DingTalkLogger adapts the DingTalk stream logger to Keelith's logger.
type DingTalkLogger struct{ logger *slog.Logger }

func NewDingTalk(logger *slog.Logger) *DingTalkLogger { return &DingTalkLogger{logger: logger} }
func (l *DingTalkLogger) Debugf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "dingtalk", formatted(format, args...))
}
func (l *DingTalkLogger) Infof(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "dingtalk", formatted(format, args...))
}
func (l *DingTalkLogger) Warningf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "dingtalk", formatted(format, args...))
}
func (l *DingTalkLogger) Errorf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelError, "dingtalk", formatted(format, args...))
}
func (l *DingTalkLogger) Fatalf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelError, "dingtalk", "fatal: "+formatted(format, args...))
}

// QQLogger adapts the BotGo logger to Keelith's logger.
type QQLogger struct{ logger *slog.Logger }

func NewQQ(logger *slog.Logger) *QQLogger { return &QQLogger{logger: logger} }
func (l *QQLogger) Debug(args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "qq", message(args...))
}
func (l *QQLogger) Info(args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "qq", message(args...))
}
func (l *QQLogger) Warn(args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "qq", message(args...))
}
func (l *QQLogger) Error(args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelError, "qq", message(args...))
}
func (l *QQLogger) Debugf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelDebug, "qq", formatted(format, args...))
}
func (l *QQLogger) Infof(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelInfo, "qq", formatted(format, args...))
}
func (l *QQLogger) Warnf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelWarn, "qq", formatted(format, args...))
}
func (l *QQLogger) Errorf(format string, args ...interface{}) {
	logValue(l.logger, context.Background(), slog.LevelError, "qq", formatted(format, args...))
}
func (l *QQLogger) Sync() error { return nil }
