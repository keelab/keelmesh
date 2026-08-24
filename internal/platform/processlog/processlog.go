package processlog

import (
	"log/slog"
	"os"
	"sync"

	"github.com/keelab/keelith/observability/logging"
	kresource "github.com/keelab/keelith/observability/resource"
)

var (
	once   sync.Once
	logger *slog.Logger
)

// Logger returns the process-boundary logger. Runtime and business code must
// use the App-scoped logger injected from observability.Bundle instead.
func Logger() *slog.Logger {
	once.Do(func() {
		handler, _, err := logging.NewHandler(os.Stderr, logging.Config{
			Level: "info", Format: logging.FormatJSON,
		})
		if err != nil {
			logger = slog.New(slog.DiscardHandler)
			return
		}
		resource, err := kresource.New(kresource.Config{ServiceName: "demo-bootstrap"})
		if err != nil {
			logger = slog.New(slog.DiscardHandler)
			return
		}
		redacter, err := logging.NewRedacter("authorization", "cookie", "set-cookie")
		if err != nil {
			logger = slog.New(slog.DiscardHandler)
			return
		}
		logger, err = logging.New(handler, resource, redacter)
		if err != nil {
			logger = slog.New(slog.DiscardHandler)
		}
	})
	return logger
}
