package gatecore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	koutbox "github.com/keelab/contrib/data/sql/outbox"
	"github.com/keelab/keelith/app"
	kclient "github.com/keelab/keelith/client"
	"github.com/keelab/keelith/governance/dependency"
	"github.com/keelab/keelith/governance/policy"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/registry"
	registrymemory "github.com/keelab/keelith/registry/memory"
	kserver "github.com/keelab/keelith/server"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	gatev1 "github.com/keelab/keelmesh/gen/gate/v1"
	gateapp "github.com/keelab/keelmesh/internal/application/gate"
	"github.com/keelab/keelmesh/internal/infrastructure/agentclient"
	"github.com/keelab/keelmesh/internal/infrastructure/channelclient"
	"github.com/keelab/keelmesh/internal/infrastructure/messaging/delivery"
	transportgrpc "github.com/keelab/keelmesh/internal/transport/grpc"
	"go.opentelemetry.io/otel/propagation"
)

const defaultGRPCAddress = "127.0.0.1:9020"
const agentRuntimeAddress = "127.0.0.1:9030"
const channelCoreAddress = "127.0.0.1:9010"

type taskEventProjector struct{ service *gateapp.Service }

func (p taskEventProjector) ProjectTaskEvent(ctx context.Context, event *agentv1.TaskEvent) error {
	progressState, progressContent := "", ""
	if event.GetProgress() != nil {
		progressState, progressContent = event.GetProgress().GetStatus(), event.GetProgress().GetPreview()
	}
	response, err := p.service.ProjectTaskEvent(ctx, &gatev1.ProjectTaskEventRequest{TaskId: event.GetTaskId(), EventId: event.GetEventId(), Sequence: event.GetSequence(), State: event.GetState(), Content: event.GetContent(), Error: event.GetError(), ProgressState: progressState, ProgressContent: progressContent})
	if err != nil {
		return err
	}
	if response != nil && response.GetReason() != "" && !response.GetDuplicate() {
		return fmt.Errorf("task event projection rejected: %s", response.GetReason())
	}
	return nil
}

func newGateImplementation(ctx context.Context) (*gateapp.Service, func(), error) {
	dsn := os.Getenv("KEELMESH_GATE_DATABASE_URL")
	if dsn == "" {
		return gateapp.New(), func() {}, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("gatecore: open task database: %w", err)
	}
	repository, err := gateapp.NewPostgresTaskRepository(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := repository.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	audit, err := gateapp.NewPostgresAuditStore(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := audit.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	implementation, err := gateapp.NewWithRepository(repository)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	implementation.SetAuditStore(audit)
	return implementation, func() { _ = db.Close() }, nil
}

func Run(ctx context.Context, output io.Writer) error {
	if ctx == nil {
		return fmt.Errorf("gatecore: context is nil")
	}
	implementation, closeTaskDB, err := newGateImplementation(ctx)
	if err != nil {
		return err
	}
	defer closeTaskDB()
	discovery := registrymemory.New()
	store, err := policyStore()
	if err != nil {
		return fmt.Errorf("gatecore: build policy: %w", err)
	}
	outbound, err := kclient.NewOutbound(kclient.OutboundConfig{Dependency: dependency.Config{Resolver: store}})
	if err != nil {
		return fmt.Errorf("gatecore: build outbound governance: %w", err)
	}
	dial, err := kgrpc.NewNodeDialer(kgrpc.NodeDialerConfig{AllowInsecure: true})
	if err != nil {
		return fmt.Errorf("gatecore: build dependency dialer: %w", err)
	}
	if err := registerDependencyInstances(ctx, discovery); err != nil {
		return fmt.Errorf("gatecore: register dependencies: %w", err)
	}
	factory, err := kgrpc.NewManagedDependencyFactory(kgrpc.ManagedDependencyFactoryConfig{Discovery: discovery, Outbound: outbound, Dial: dial})
	if err != nil {
		return fmt.Errorf("gatecore: build dependency factory: %w", err)
	}
	agentManaged, err := factory.New("gate.agent-runtime", "agent.v1.AgentRuntimeService")
	if err != nil {
		return fmt.Errorf("gatecore: build AgentRuntime dependency: %w", err)
	}
	channelManaged, err := factory.New("gate.channel-core", "channel.v1.ChannelService")
	if err != nil {
		return fmt.Errorf("gatecore: build ChannelCore dependency: %w", err)
	}
	agent, err := agentclient.NewManaged(agentManaged)
	if err != nil {
		return fmt.Errorf("gatecore: build AgentRuntime client: %w", err)
	}
	implementation.SetExecutor(agent)
	channel, err := channelclient.NewManaged(channelManaged)
	if err != nil {
		return fmt.Errorf("gatecore: build ChannelCore client: %w", err)
	}
	outboxRuntime, closeOutbox, err := newDeliveryRuntime(ctx, channel)
	if err != nil {
		_ = agent.Close()
		_ = channel.Close()
		return err
	}
	defer closeOutbox()
	if outboxRuntime != nil {
		implementation.SetOutboundDispatcher(outboxRuntime.dispatcher)
	} else {
		implementation.SetOutboundDispatcher(channel)
	}
	agent.SetEventProjector(taskEventProjector{service: implementation})
	profile, err := service.NewProfile(
		"private-api",
		service.NewGroup("gate").Bind(gatev1.BindChannelGateService(implementation)),
	)
	if err != nil {
		_ = agent.Close()
		_ = channel.Close()
		return fmt.Errorf("gatecore: build service profile: %w", err)
	}
	surface, err := profile.GRPC("gatecore-grpc")
	if err != nil {
		_ = agent.Close()
		_ = channel.Close()
		return fmt.Errorf("gatecore: build gRPC surface: %w", err)
	}
	healthRegistry := health.NewRegistry()
	if err := healthRegistry.Register(health.KindDependency, "gate-contract", func(context.Context) health.Result {
		return health.Pass("gate contract ready")
	}); err != nil {
		return fmt.Errorf("gatecore: register health: %w", err)
	}
	streamBundle, err := middleware.NewStreamBundle()
	if err != nil {
		_ = agent.Close()
		_ = channel.Close()
		return fmt.Errorf("gatecore: build stream middleware: %w", err)
	}
	listener, err := net.Listen("tcp", defaultGRPCAddress)
	if err != nil {
		_ = agent.Close()
		_ = channel.Close()
		return fmt.Errorf("gatecore: listen %s: %w", defaultGRPCAddress, err)
	}
	server, err := transportgrpc.NewServer(&transportgrpc.Service{
		Listener:       listener,
		Surface:        surface,
		HealthRegistry: healthRegistry,
		StreamBundle:   streamBundle,
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		_ = listener.Close()
		_ = agent.Close()
		_ = channel.Close()
		return fmt.Errorf("gatecore: build gRPC server: %w", err)
	}
	if output != nil {
		_, _ = fmt.Fprintf(output, "gatecore listening on %s\n", defaultGRPCAddress)
	}
	servers := make([]kserver.Server, 0, 1)
	if outboxRuntime != nil {
		servers = append(servers, outboxRuntime.runtime.Dispatcher())
	}
	application, err := app.New(
		app.WithHealth(healthRegistry),
		app.WithComponents(agentManaged, channelManaged, app.ComponentFunc{
			ComponentName: "gate.recovery",
			DependsOn:     []string{agentManaged.Name(), channelManaged.Name()},
			StartFunc: func(startCtx context.Context) error {
				return implementation.RecoverPending(startCtx)
			},
		}),
		app.WithServers(servers...),
		app.WithServers(server),
	)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("gatecore: build application: %w", err)
	}
	return errors.Join(application.Run(ctx), agent.Close(), channel.Close())
}

type deliveryRuntime struct {
	runtime    *koutbox.Runtime
	dispatcher gateapp.OutboundDispatcher
}

func newDeliveryRuntime(ctx context.Context, channel *channelclient.Client) (*deliveryRuntime, func(), error) {
	dsn := os.Getenv("KEELMESH_GATE_DATABASE_URL")
	if dsn == "" || channel == nil {
		return nil, func() {}, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("gatecore: open delivery database: %w", err)
	}
	config := koutbox.RuntimeConfig{Table: "keelmesh_gate_outbox", Isolation: "read-committed", PollInterval: 250 * time.Millisecond, ErrorDelay: time.Second, LeaseTTL: 30 * time.Second, PublishTimeout: 10 * time.Second, BatchSize: 100, MaxAttempts: 20, RetryBase: time.Second, RetryMax: time.Minute}
	schema, err := koutbox.Schema(config.Table)
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("gatecore: build delivery schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("gatecore: ensure delivery schema: %w", err)
	}
	router := delivery.NewRouter()
	publisher, err := gateapp.NewDeliveryPublisher(channel)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := router.Register("channelcore.delivery.final", publisher); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("gatecore: register final delivery: %w", err)
	}
	if err := router.Register("channelcore.delivery.progress", publisher); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("gatecore: register progress delivery: %w", err)
	}
	runtime, err := koutbox.NewRuntime(config, "gate.delivery", "gatecore", db, router)
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("gatecore: build delivery outbox: %w", err)
	}
	dispatcher, err := gateapp.NewOutboxDelivery(db, runtime, channel)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return &deliveryRuntime{runtime: runtime, dispatcher: dispatcher}, func() { _ = db.Close() }, nil
}

func policyStore() (*policy.Store, error) {
	return policy.NewStore(policy.Definition{Revision: "keelmesh-gate-v1", Global: policy.Default()})
}

func registerDependencyInstances(ctx context.Context, discovery *registrymemory.Registry) error {
	instances := []struct {
		id      string
		service string
		address string
	}{
		{"agent-runtime-1", "agent.v1.AgentRuntimeService", agentRuntimeAddress},
		{"channel-core-1", "channel.v1.ChannelService", channelCoreAddress},
	}
	for _, item := range instances {
		instance, err := registry.NewInstance(item.id, item.service, []string{"grpc://" + item.address}, nil)
		if err != nil {
			return fmt.Errorf("create %s instance: %w", item.service, err)
		}
		if err := discovery.Register(ctx, instance); err != nil {
			return fmt.Errorf("register %s instance: %w", item.service, err)
		}
	}
	return nil
}
