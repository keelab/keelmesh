package channelcore

import (
	"fmt"

	kmiddleware "github.com/keelab/keelith/middleware"
	grpcinterceptor "github.com/keelab/keelmesh/internal/transport/grpc/interceptor"
	httpmiddleware "github.com/keelab/keelmesh/internal/transport/http/middleware"
	"github.com/keelab/keelmesh/internal/transport/middleware"
)

type protocolPolicies struct {
	requestID       *kmiddleware.Bundle
	httpIdempotency *kmiddleware.Bundle
	grpcDeadline    *kmiddleware.Bundle
}

func newProtocolPolicies() (protocolPolicies, error) {
	requestID, err := middleware.NewRequestIDBundle()
	if err != nil {
		return protocolPolicies{}, fmt.Errorf("build request ID middleware: %w", err)
	}
	httpIdempotency, err := httpmiddleware.NewTaskServiceBundle()
	if err != nil {
		return protocolPolicies{}, fmt.Errorf("build HTTP middleware: %w", err)
	}
	grpcDeadline, err := grpcinterceptor.NewTaskServiceBundle()
	if err != nil {
		return protocolPolicies{}, fmt.Errorf("build gRPC middleware: %w", err)
	}

	return protocolPolicies{
		requestID:       requestID,
		httpIdempotency: httpIdempotency,
		grpcDeadline:    grpcDeadline,
	}, nil
}
