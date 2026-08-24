package dependencies

import (
	kcache "github.com/keelab/contrib/cache/redis"
	kgorm "github.com/keelab/contrib/data/gorm"
	ksql "github.com/keelab/contrib/data/sql"
	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/ops"
	kserver "github.com/keelab/keelith/server"
)

// Components returns a defensive lifecycle graph snapshot.
func (r *Resources) Components() []app.Component {
	if r == nil {
		return nil
	}
	return append([]app.Component(nil), r.components...)
}

// RuntimeStatuses returns low-sensitive operational status providers.
func (r *Resources) RuntimeStatuses() []ops.RuntimeStatusRegistration {
	if r == nil {
		return nil
	}
	return append([]ops.RuntimeStatusRegistration(nil), r.statuses...)
}

// Database returns the Keelith-instrumented PostgreSQL pool.
func (r *Resources) Database() *ksql.Database {
	if r == nil {
		return nil
	}
	return r.database
}

// GORM returns the Keelith-managed GORM handle that borrows the SQL pool.
func (r *Resources) GORM() *kgorm.Database {
	if r == nil {
		return nil
	}
	return r.gorm
}

// Cache returns the Keelith Redis cache adapter.
func (r *Resources) Cache() *kcache.Client {
	if r == nil {
		return nil
	}
	return r.cache
}

// Servers returns background runtimes that start after components and stop
// before Kafka, Redis, and SQL resources.
func (r *Resources) Servers() []kserver.Server {
	if r == nil {
		return nil
	}
	return append([]kserver.Server(nil), r.servers...)
}
