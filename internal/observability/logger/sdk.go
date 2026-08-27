package logger

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/keelab/keelith/observability/logging"
)

var sensitiveQuery = regexp.MustCompile(`(?i)([?&](?:access[_-]?key|access[_-]?token|client[_-]?secret|ticket|token|secret|password|authorization|corp[_-]?secret|app[_-]?secret)=)[^&\s"']+`)
var sensitiveField = regexp.MustCompile(`(?i)(["']?(?:access[_-]?key|access[_-]?token|client[_-]?secret|ticket|token|secret|password|authorization|corp[_-]?secret|app[_-]?secret)["']?\s*[:=]\s*["']?)[^,\s&}"']+`)

func logValue(logger *logging.Logger, ctx context.Context, level slog.Level, sdk, value string) {
	if logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	logger.WithCallerSkip(1).Log(ctx, level, "sdk log", "sdk", sdk, "message", value)
}

// sanitize sanitizes the value by redacting sensitive information.
func sanitize(value string) string {
	value = sensitiveQuery.ReplaceAllString(value, `${1}[REDACTED]`)
	return sensitiveField.ReplaceAllString(value, `${1}[REDACTED]`)
}

// message returns a sanitized message.
func message(args ...any) string {
	return sanitize(fmt.Sprint(args...))
}

// formatted returns a sanitized formatted message.
func formatted(format string, args ...any) string {
	return sanitize(fmt.Sprintf(format, args...))
}
