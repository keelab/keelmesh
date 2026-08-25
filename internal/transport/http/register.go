package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/keelab/keelith/operation"
	khttp "github.com/keelab/keelith/transport/http"
)

// Registrar contributes protocol-specific callback routes to one shared HTTP router.
type Registrar interface {
	RegisterHTTP(*khttp.Router) error
}

// Register adapts an existing protocol-specific http.Handler to a Keelith route.
// The handler keeps ownership of protocol parsing and response semantics while
// Keelith owns request limits, metadata, observability, and lifecycle.
func Register(router *khttp.Router, method, pattern, operationMethod string, handler http.Handler) error {
	if handler == nil {
		return fmt.Errorf("httproute: handler is nil")
	}
	target, err := operation.New("http", "channel-inbound", operationMethod, operation.KindUnary)
	if err != nil {
		return err
	}
	return router.Handle(
		method,
		pattern,
		target,
		func(request *http.Request) (any, error) {
			return request, nil
		},
		func(_ context.Context, value any) (any, error) {
			request, ok := value.(*http.Request)
			if !ok {
				return nil, fmt.Errorf("httproute: unexpected request type %T", value)
			}
			writer := newRecorder()
			handler.ServeHTTP(writer, request)
			return writer, nil
		},
		func(_ context.Context, writer http.ResponseWriter, value any) error {
			response, ok := value.(*recorder)
			if !ok {
				return fmt.Errorf("httproute: unexpected response type %T", value)
			}
			for key, values := range response.header {
				for _, item := range values {
					writer.Header().Add(key, item)
				}
			}
			writer.WriteHeader(response.status)
			_, err := writer.Write(response.body)
			return err
		},
	)
}

type recorder struct {
	header      http.Header
	status      int
	body        []byte
	wroteHeader bool
}

func newRecorder() *recorder {
	return &recorder{header: make(http.Header), status: http.StatusOK}
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
}

func (r *recorder) Write(body []byte) (int, error) {
	r.wroteHeader = true
	r.body = append(r.body, body...)
	return len(body), nil
}
