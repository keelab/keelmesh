package agentcore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/service"
	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	agentapp "github.com/keelab/keelmesh/internal/application/agent"
	transportgrpc "github.com/keelab/keelmesh/internal/transport/grpc"
	transporthttp "github.com/keelab/keelmesh/internal/transport/http"
	"go.opentelemetry.io/otel/propagation"
)

const defaultGRPCAddress = "127.0.0.1:9030"

func Run(ctx context.Context, output io.Writer) error {
	implementation, closeDB, err := newAgentImplementation(ctx)
	if err != nil {
		return err
	}
	defer closeDB()
	if os.Getenv("KEELMESH_AGENT_BACKEND") == "echo" {
		implementation.SetBackend(agentapp.EchoBackend{})
	} else if os.Getenv("KEELMESH_AGENT_BACKEND") == "openai" {
		providerHTTP, httpErr := transporthttp.New(
			&stdhttp.Client{Timeout: 90 * time.Second},
			nil,
			metadata.Policy{},
			propagation.TraceContext{},
			8<<20,
		)
		if httpErr != nil {
			return fmt.Errorf("agentcore: build provider HTTP client: %w", httpErr)
		}
		backend, backendErr := agentapp.NewOpenAIBackend(
			os.Getenv("KEELMESH_AGENT_OPENAI_BASE_URL"),
			os.Getenv("KEELMESH_AGENT_OPENAI_API_KEY"),
			os.Getenv("KEELMESH_AGENT_OPENAI_MODEL"),
			providerHTTP,
		)
		if backendErr != nil {
			return backendErr
		}
		implementation.SetBackend(backend)
	} else if strings.TrimSpace(os.Getenv("KEELMESH_AGENT_BACKEND")) != "" {
		return fmt.Errorf("agentcore: unsupported backend %q", os.Getenv("KEELMESH_AGENT_BACKEND"))
	}
	profile, err := service.NewProfile("private-api", service.NewGroup("agent").Bind(agentv1.BindAgentRuntimeService(implementation)))
	if err != nil {
		return fmt.Errorf("agentcore: build service profile: %w", err)
	}
	surface, err := profile.GRPC("agentcore-grpc")
	if err != nil {
		return fmt.Errorf("agentcore: build gRPC surface: %w", err)
	}
	healthRegistry := health.NewRegistry()
	streamBundle, err := middleware.NewStreamBundle()
	if err != nil {
		return fmt.Errorf("agentcore: build stream middleware: %w", err)
	}
	listener, err := net.Listen("tcp", defaultGRPCAddress)
	if err != nil {
		return fmt.Errorf("agentcore: listen %s: %w", defaultGRPCAddress, err)
	}
	server, err := transportgrpc.NewServer(&transportgrpc.Service{
		Listener: listener, Surface: surface, HealthRegistry: healthRegistry,
		StreamBundle: streamBundle, Propagator: propagation.TraceContext{},
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("agentcore: build gRPC server: %w", err)
	}
	if output != nil {
		_, _ = fmt.Fprintf(output, "agentcore listening on %s\n", defaultGRPCAddress)
	}
	application, err := app.New(app.WithHealth(healthRegistry), app.WithServers(server))
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("agentcore: build application: %w", err)
	}
	return application.Run(ctx)
}

func newAgentImplementation(ctx context.Context) (*agentapp.Service, func(), error) {
	dsn := os.Getenv("KEELMESH_AGENT_DATABASE_URL")
	if dsn == "" {
		return agentapp.New(), func() {}, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("agentcore: open event database: %w", err)
	}
	store, err := agentapp.NewPostgresEventStore(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := store.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	implementation, err := agentapp.NewWithEventStore(store)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return implementation, func() { _ = db.Close() }, nil
}
