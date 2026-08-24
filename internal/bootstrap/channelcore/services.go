package channelcore

import (
	"context"
	"fmt"

	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	"github.com/keelab/keelith/service"
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
) (*di.Graph, *service.Profile, error) {
	graph, _, err := newServiceHandlers(ctx, loaded.Runtime, resources, loggingDependencies, auditLogger)
	if err != nil {
		return nil, nil, fmt.Errorf("build service dependency graph: %w", err)
	}
	policies, err := newProtocolPolicies()
	if err != nil {
		_ = graph.Close(context.WithoutCancel(ctx))
		return nil, nil, fmt.Errorf("build protocol policies: %w", err)
	}

	profile, err := service.NewProfile(
		"public-api",
		service.NewGroup("authenticated").
			RequireHTTP(service.CapabilityRequestID).
			RequireGRPC(service.CapabilityRequestID).
			UseHTTPPolicies(service.NewPolicy(
				policies.requestID,
				service.CapabilityRequestID,
			)).
			UseGRPCPolicies(service.NewPolicy(
				policies.requestID,
				service.CapabilityRequestID,
			)).
			Bind(
			//taskv1.BindTaskService(
			//	handlers.Task,
			//	service.WithHTTPBundle(policies.httpIdempotency),
			//	service.WithGRPCBundle(policies.grpcDeadline),
			//),
			//orderv1.BindOrderService(handlers.Order),
			),
		service.NewGroup("public").
			RequireHTTP(service.CapabilityRequestID).
			RequireGRPC(service.CapabilityRequestID).
			UseHTTPPolicies(service.NewPolicy(
				policies.requestID,
				service.CapabilityRequestID,
			)).
			UseGRPCPolicies(service.NewPolicy(
				policies.requestID,
				service.CapabilityRequestID,
			)).
			Bind(
			//inventoryv1.BindInventoryService(handlers.Inventory),
			//customerv1.BindCustomerService(handlers.Customer),
			),
	)
	if err != nil {
		_ = graph.Close(context.WithoutCancel(ctx))
		return nil, nil, err
	}
	return graph, profile, nil
}
