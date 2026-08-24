package http

import (
	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/service"
	khttp "github.com/keelab/keelith/transport/http"
	"go.opentelemetry.io/otel/propagation"
)

type Service struct {
	Surface    *service.Surface
	Bundle     *middleware.Bundle
	Policy     metadata.Policy
	Propagator propagation.TextMapPropagator
}

// NewRouter creates the Keelith HTTP router and applies one service profile.
func NewRouter(service *Service) (*khttp.Router, error) {
	combinedBundle, err := service.Surface.Compose(service.Bundle)
	if err != nil {
		return nil, err
	}
	router, err := khttp.NewRouter(
		khttp.WithMiddleware(combinedBundle),
		khttp.WithMetadataPolicy(service.Policy),
		khttp.WithPropagator(service.Propagator),
		khttp.WithMaxResponseBytes(1<<20),
	)
	if err != nil {
		return nil, err
	}
	if err = service.Surface.RegisterHTTP(router); err != nil {
		return nil, err
	}
	return router, nil
}
