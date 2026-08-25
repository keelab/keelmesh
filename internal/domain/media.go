package domain

import (
	"context"
	"io"
)

// MediaDomain is the channelcore-owned boundary for attachment lifetime.
// Channels receive managed references, never arbitrary filesystem paths.
type MediaDomain interface {
	Store(context.Context, string, string, io.Reader) (MediaPartEntity, error)
	Open(context.Context, string) (MediaEntity, error)
	Release(context.Context, string) error
}
type MediaEntity struct {
	Reader      io.ReadCloser
	Filename    string
	ContentType string
}
