package channelcore

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	"github.com/keelab/keelith/service"
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
	"github.com/keelab/keelmesh/internal/config"
	"github.com/keelab/keelmesh/internal/infrastructure/dependencies"
)

// newServiceProfile is the explicit, strongly typed list of business services
// exposed by this process. Generated BindXxx functions own transport wiring;
// this composition root only constructs implementations and protocol policy.
func newServiceProfile(
	ctx context.Context,
	loaded config.ChannelLoaded,
	resources *dependencies.Resources,
	loggingDependencies logging.Dependencies,
	auditLogger *audit.Logger,
	channels *channelruntime.Runtime,
) (*di.Graph, *service.Profile, error) {
	graph, handlers, err := newServiceHandlers(ctx, loaded.Runtime, resources, channels, loggingDependencies, auditLogger)
	if err != nil {
		return nil, nil, fmt.Errorf("build service dependency graph: %w", err)
	}
	policies, err := newProtocolPolicies()
	if err != nil {
		_ = graph.Close(context.WithoutCancel(ctx))
		return nil, nil, fmt.Errorf("build protocol policies: %w", err)
	}
	profile, err := service.NewProfile("public-api", service.NewGroup("channel").
		RequireGRPC(service.CapabilityRequestID).
		UseGRPCPolicies(service.NewPolicy(policies.requestID, service.CapabilityRequestID)).
		Bind(channelv1.BindChannelService(handlers.Channel)))
	if err != nil {
		_ = graph.Close(context.WithoutCancel(ctx))
		return nil, nil, err
	}
	return graph, profile, nil
}
