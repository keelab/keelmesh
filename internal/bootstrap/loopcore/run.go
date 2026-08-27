package loopcore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/keelab/keelith/app"
	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/dependency"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/registry"
	registrymemory "github.com/keelab/keelith/registry/memory"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	loopv1 "github.com/keelab/keelmesh/gen/loop/v1"
	loopapp "github.com/keelab/keelmesh/internal/application/loop"
	"github.com/keelab/keelmesh/internal/infrastructure/agentclient"
	"github.com/keelab/keelmesh/internal/infrastructure/channelclient"
	"github.com/keelab/keelmesh/internal/infrastructure/gateclient"
	transportgrpc "github.com/keelab/keelmesh/internal/transport/grpc"
	"go.opentelemetry.io/otel/propagation"
)

func Run(ctx context.Context, output io.Writer) error {
	if ctx == nil {
		return fmt.Errorf("loopcore: context is nil")
	}
	discovery := registrymemory.New()
	for _, item := range []struct{ id, service, address string }{
		{"channel-core-1", "channel.v1.ChannelService", "127.0.0.1:9010"},
		{"gate-core-1", "gate.v1.ChannelGateService", "127.0.0.1:9020"},
		{"agent-runtime-1", "agent.v1.AgentRuntimeService", "127.0.0.1:9030"},
	} {
		instance, err := registry.NewInstance(item.id, item.service, []string{"grpc://" + item.address}, nil)
		if err != nil {
			return fmt.Errorf("loopcore: create %s instance: %w", item.service, err)
		}
		if err := discovery.Register(ctx, instance); err != nil {
			return fmt.Errorf("loopcore: register %s instance: %w", item.service, err)
		}
	}
	store, err := policy.NewStore(policy.Definition{Revision: "keelmesh-loop-v1", Global: policy.Default()})
	if err != nil {
		return fmt.Errorf("loopcore: build policy: %w", err)
	}
	outbound, err := kclient.NewOutbound(kclient.OutboundConfig{Dependency: dependency.Config{Resolver: store}})
	if err != nil {
		return fmt.Errorf("loopcore: build outbound: %w", err)
	}
	dial, err := kgrpc.NewNodeDialer(kgrpc.NodeDialerConfig{AllowInsecure: true})
	if err != nil {
		return fmt.Errorf("loopcore: build dialer: %w", err)
	}
	factory, err := kgrpc.NewManagedDependencyFactory(kgrpc.ManagedDependencyFactoryConfig{Discovery: discovery, Outbound: outbound, Dial: dial})
	if err != nil {
		return fmt.Errorf("loopcore: build dependency factory: %w", err)
	}
	channelDependency, err := factory.New("loop.channel-core", "channel.v1.ChannelService")
	if err != nil {
		return fmt.Errorf("loopcore: build ChannelCore dependency: %w", err)
	}
	gateDependency, err := factory.New("loop.gate-core", "gate.v1.ChannelGateService")
	if err != nil {
		return fmt.Errorf("loopcore: build GateCore dependency: %w", err)
	}
	agentDependency, err := factory.New("loop.agent-runtime", "agent.v1.AgentRuntimeService")
	if err != nil {
		return fmt.Errorf("loopcore: build AgentRuntime dependency: %w", err)
	}
	channel, err := channelclient.NewManaged(channelDependency)
	if err != nil {
		return fmt.Errorf("loopcore: build ChannelCore client: %w", err)
	}
	gate, err := gateclient.NewManaged(gateDependency)
	if err != nil {
		return fmt.Errorf("loopcore: build GateCore client: %w", err)
	}
	agent, err := agentclient.NewManaged(agentDependency)
	if err != nil {
		return fmt.Errorf("loopcore: build AgentRuntime client: %w", err)
	}
	loopService, closeDB, err := newLoopImplementation(ctx)
	if err != nil {
		return err
	}
	defer closeDB()
	bridge := &bridge{channel: channel.API(), gate: gate, loop: loopService, agent: agent}
	profile, err := service.NewProfile(
		"private-api",
		service.NewGroup("loop").Bind(loopv1.BindLoopService(loopService)),
	)
	if err != nil {
		return fmt.Errorf("loopcore: build service profile: %w", err)
	}
	surface, err := profile.GRPC("loopcore-grpc")
	if err != nil {
		return fmt.Errorf("loopcore: build gRPC surface: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:9040")
	if err != nil {
		return fmt.Errorf("loopcore: listen gRPC: %w", err)
	}
	streamBundle, err := middleware.NewStreamBundle()
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("loopcore: build stream middleware: %w", err)
	}
	server, err := transportgrpc.NewServer(&transportgrpc.Service{
		Listener: listener, Surface: surface, HealthRegistry: health.NewRegistry(),
		StreamBundle: streamBundle, Propagator: propagation.TraceContext{},
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("loopcore: build gRPC server: %w", err)
	}
	application, err := app.New(app.WithComponents(channelDependency, gateDependency, agentDependency, bridge), app.WithServers(server))
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("loopcore: build application: %w", err)
	}
	if output != nil {
		_, _ = fmt.Fprintln(output, "loopcore inbound bridge ready")
	}
	return application.Run(ctx)
}

func newLoopImplementation(ctx context.Context) (*loopapp.Service, func(), error) {
	dsn := os.Getenv("KEELMESH_LOOP_DATABASE_URL")
	if dsn == "" {
		return loopapp.New(), func() {}, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("loopcore: open run database: %w", err)
	}
	repository, err := loopapp.NewPostgresRepository(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := repository.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	service, err := loopapp.NewWithRepository(repository)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return service, func() { _ = db.Close() }, nil
}
