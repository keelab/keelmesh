package grpc

import (
	"net"

	"github.com/keelab/keelith/health"
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/service"
	kgrpc "github.com/keelab/keelith/transport/grpc"
	"go.opentelemetry.io/otel/propagation"
)

type Service struct {
	Address        string
	Listener       net.Listener
	Surface        *service.Surface
	HealthRegistry *health.Registry
	Bundle         *middleware.Bundle
	StreamBundle   *middleware.StreamBundle
	Policy         metadata.Policy
	Propagator     propagation.TextMapPropagator
}

// NewServer creates a bounded Keelith gRPC server and applies one service profile.
func NewServer(service *Service) (*kgrpc.Server, error) {
	combinedBundle, err := service.Surface.Compose(service.Bundle)
	if err != nil {
		return nil, err
	}
	var endpoint kgrpc.ServerOption
	if service.Listener == nil {
		endpoint = kgrpc.WithAddress(service.Address)
	} else {
		endpoint = kgrpc.WithListener(service.Listener)
	}
	server, err := kgrpc.NewServer(
		kgrpc.WithName(service.Surface.Name()),
		endpoint,
		kgrpc.WithHealth(service.HealthRegistry),
		kgrpc.WithMiddleware(combinedBundle),
		kgrpc.WithStreamMiddleware(service.StreamBundle),
		kgrpc.WithMetadataPolicy(service.Policy),
		kgrpc.WithPropagator(service.Propagator),
		kgrpc.WithMaxReceiveMessageBytes(1<<20),
		kgrpc.WithMaxSendMessageBytes(1<<20),
	)
	if err != nil {
		return nil, err
	}
	if err = service.Surface.RegisterGRPC(server.Registrar()); err != nil {
		return nil, err
	}
	return server, nil
}
