package dependencies

import (
	"context"
	"errors"

	kcache "github.com/keelab/contrib/cache/redis"
	kgorm "github.com/keelab/contrib/data/gorm"
	ksql "github.com/keelab/contrib/data/sql"
	koutbox "github.com/keelab/contrib/data/sql/outbox"
	kredis "github.com/keelab/contrib/redis"
	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/ops"
	kserver "github.com/keelab/keelith/server"
)

// Resources owns the optional external dependency graph.
type Resources struct {
	database   *ksql.Database
	gorm       *kgorm.Database
	redis      *kredis.Client
	cache      *kcache.Client
	outbox     *koutbox.Runtime
	components []app.Component
	servers    []kserver.Server
	statuses   []ops.RuntimeStatusRegistration
}

// Shutdown releases constructed resources in reverse dependency order.
func (r *Resources) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	failures := make([]error, 0, 4)

	if r.redis != nil {
		failures = append(failures, r.redis.Shutdown(ctx))
	}
	if r.gorm != nil {
		failures = append(failures, r.gorm.Shutdown(ctx))
	}
	if r.database != nil {
		failures = append(failures, r.database.Shutdown(ctx))
	}
	return errors.Join(failures...)
}
