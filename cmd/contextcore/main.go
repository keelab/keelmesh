package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/keelab/keelith/app/termination"
	"github.com/keelab/keelmesh/internal/bootstrap/contextcore"
	"github.com/keelab/keelmesh/internal/platform/processlog"
)

func main() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	err := termination.Run(context.Background(), signals, func(ctx context.Context) error {
		return contextcore.Run(ctx, os.Stdout)
	})
	signal.Stop(signals)
	if errors.Is(err, termination.ErrForcedShutdown) {
		processlog.Logger().Error("forced shutdown after second signal")
		os.Exit(2)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintf(os.Stderr, "channelcore failed: %v\n", err)
		os.Exit(1)
	}
}
