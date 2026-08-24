package channelcore

import (
	"context"
	"fmt"
	"io"

	"github.com/keelab/keelmesh/internal/transport/grpc"
)

func Run(ctx context.Context, output io.Writer) error {
	r, err := NewRuntime(ctx, output)
	if err != nil {
		return err
	}
	// expose gRPC surface
	surface, err := r.Profile.GRPC("channelcore-grpc")
	if err != nil {
		_ = r.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("build gRPC surface: %w", err)
	}
	apiServer, err := grpc.NewServer(&grpc.Service{
		Address:        r.Config.GRPCAddr,
		Surface:        surface,
		HealthRegistry: r.Health,
		Bundle:         r.Middleware,
		StreamBundle:   r.Stream,
		Policy:         r.Metadata,
		Propagator:     r.Telemetry.Propagator(),
	})

	if err != nil {
		_ = r.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("build gRPC server: %w", err)
	}
	return buildApp(ctx, r, apiServer, r.Config.GRPCOpsAddr, surface)
}
