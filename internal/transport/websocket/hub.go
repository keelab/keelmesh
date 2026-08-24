package websocket

import (
	"fmt"

	"github.com/keelab/keelith/middleware"
	"github.com/keelab/keelith/operation"
	kwebsocket "github.com/keelab/keelith/transport/websocket"
)

const Protocol = "ws"

type Service struct {
	Options kwebsocket.Options
	Streams *middleware.StreamBundle
}

// NewOperation creates a ws bidi-stream operation for websocket routes.
func NewOperation(service, method string, kind operation.Kind) (operation.Operation, error) {
	return operation.New(Protocol, service, method, kind)
}

// NewHub creates a bounded Keelith WebSocket hub and applies one service profile.
func NewHub(service *Service) (*kwebsocket.Hub, error) {
	if service == nil {
		return nil, fmt.Errorf("%w: service is nil", kwebsocket.ErrInvalidOption)
	}
	return kwebsocket.NewHub(service.Options, service.Streams)
}
