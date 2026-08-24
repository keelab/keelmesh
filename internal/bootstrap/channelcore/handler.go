package channelcore

import (
	"context"
	"log/slog"

	"github.com/keelab/keelith/di"
	"github.com/keelab/keelith/observability/logging"
	"github.com/keelab/keelith/observability/logging/audit"
	channelv1 "github.com/keelab/keelmesh/gen/channel/v1"
	"github.com/keelab/keelmesh/internal/application/channel"
	channelruntime "github.com/keelab/keelmesh/internal/channelcore"
	"github.com/keelab/keelmesh/internal/config"
	"github.com/keelab/keelmesh/internal/infrastructure/dependencies"
)

// ServiceHandlers is the strongly typed DI graph root consumed by Profile.
type ServiceHandlers struct {
	di.Roots

	Channel channelv1.ChannelServiceKeelithServer
	//Order     orderv1.OrderServiceKeelithServer
	//Inventory inventoryv1.InventoryServiceKeelithServer
	//Customer  customerv1.CustomerServiceKeelithServer
	Logger  *slog.Logger
	Logging *logging.Controller
	Audit   *audit.Logger
}

// ServiceInputs contains the stable process-scoped values shared by business
// DI modules. It must not be used as a general-purpose service locator.
type ServiceInputs struct {
	Config    config.ChannelConfig
	Resources *dependencies.Resources
	Logger    *slog.Logger
	Logging   *logging.Controller
	Audit     *audit.Logger
	Channels  *channelruntime.Runtime
}

func newServiceHandlers(ctx context.Context, cfg config.ChannelConfig, resources *dependencies.Resources, channels *channelruntime.Runtime, loggingDependencies logging.Dependencies, auditLogger *audit.Logger) (*di.Graph, ServiceHandlers, error) {
	inputs := ServiceInputs{
		Config: cfg, Resources: resources,
		Logger:   loggingDependencies.Logger,
		Logging:  loggingDependencies.Controller,
		Audit:    auditLogger,
		Channels: channels,
	}
	return di.BuildRoots[ServiceHandlers](ctx, ServiceModule(inputs))
}

// ServiceModule returns the complete scoped business module used by both the
// runtime graph and the isolated wiring frontend.
func ServiceModule(inputs ServiceInputs) di.Module {
	plugins := businessPlugins()
	return di.MustModule("demo.runtime",
		di.Value(inputs),
		di.Value(inputs.Logger),
		di.Value(inputs.Logging),
		di.Value(inputs.Audit),
		di.IncludePlugins(plugins...),
		di.Value(inputs.Channels),
		di.Provide(channel.New, di.As((*channelv1.ChannelServiceKeelithServer)(nil))),
		di.Export((*channelv1.ChannelServiceKeelithServer)(nil)),
		di.Export((**slog.Logger)(nil)),
		di.Export((**logging.Controller)(nil)),
		di.Export((**audit.Logger)(nil)),
	)
}

func businessPlugins() []di.ModulePlugin {
	return nil

	/*
		//return []di.ModulePlugin{
		//	{
		//		Plugin: di.Plugin{ID: "order", Priority: 100, Capabilities: []string{"grpc", "http"}},
		//		Module: di.MustModule("plugin.order",
		//			di.Provide(applicationorder.NewService),
		//			di.Provide(handlerorder.New, di.As((*orderv1.OrderServiceKeelithServer)(nil))),
		//			di.Export((*orderv1.OrderServiceKeelithServer)(nil)),
		//		),
		//	},
		//	{
		//		Plugin: di.Plugin{ID: "inventory", Priority: 200, Capabilities: []string{"grpc", "http"}},
		//		Module: di.MustModule("plugin.inventory",
		//			di.Provide(applicationinventory.NewService),
		//			di.Provide(handlerinventory.New, di.As((*inventoryv1.InventoryServiceKeelithServer)(nil))),
		//			di.Export((*inventoryv1.InventoryServiceKeelithServer)(nil)),
		//		),
		//	},
		//	{
		//		Plugin: di.Plugin{ID: "customer", Priority: 300, Capabilities: []string{"grpc", "http"}},
		//		Module: di.MustModule("plugin.customer",
		//			di.Provide(applicationcustomer.NewService),
		//			di.Provide(handlercustomer.New, di.As((*customerv1.CustomerServiceKeelithServer)(nil))),
		//			di.Export((*customerv1.CustomerServiceKeelithServer)(nil)),
		//		),
		//	},
		//}
		return nil*/
}
