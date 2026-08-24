package dependencies

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	kcache "github.com/keelab/contrib/cache/redis"
	kgorm "github.com/keelab/contrib/data/gorm"
	ksql "github.com/keelab/contrib/data/sql"
	koutbox "github.com/keelab/contrib/data/sql/outbox"
	kredis "github.com/keelab/contrib/redis"
	"github.com/keelab/keelith/app"
	"github.com/keelab/keelith/observability"
	"github.com/keelab/keelith/ops"
	"github.com/keelab/keelith/secret"
	secretfile "github.com/keelab/keelith/secret/file"
	"github.com/keelab/keelmesh/internal/config"
	"github.com/keelab/keelmesh/internal/infrastructure/messaging/delivery"
	"gorm.io/driver/postgres"
	gormio "gorm.io/gorm"

	// Register the pgx database/sql driver selected by the runtime config.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Build constructs clients without touching the network. Connectivity checks
// run later inside app.App's startup rollback boundary.
func Build(ctx context.Context, loaded config.ChannelLoaded, telemetry *observability.Bundle) (*Resources, error) {
	resources := &Resources{}
	if !loaded.Runtime.DependenciesEnabled {
		return resources, nil
	}
	provider, err := secretfile.New(secretfile.Config{Root: loaded.Runtime.SecretRoot})
	if err != nil {
		return nil, fmt.Errorf("dependencies: open secret provider: %w", err)
	}
	secrets, err := secret.NewManager(secret.Registration{Name: "file", Provider: provider})
	if err != nil {
		return nil, fmt.Errorf("dependencies: create secret manager: %w", err)
	}
	rollback := func(cause error) (*Resources, error) {
		return nil, errors.Join(cause, resources.Shutdown(context.WithoutCancel(ctx)))
	}

	sqlConfig, ok := loaded.SQL.Current()
	if !ok {
		return rollback(fmt.Errorf("dependencies: SQL config is unavailable"))
	}
	database, err := ksql.OpenConfigured(
		ctx,
		sqlConfig,
		secrets,
		ksql.WithTracerProvider(telemetry.TracerProvider()),
		ksql.WithMeterProvider(telemetry.MeterProvider()),
	)
	if err != nil {
		return rollback(fmt.Errorf("dependencies: build PostgreSQL: %w", err))
	}
	resources.database = database
	if err := loaded.SQL.BindApply(database.ApplyConnectionConfig); err != nil {
		return rollback(fmt.Errorf("dependencies: bind SQL pool reload: %w", err))
	}
	gormHandle, err := gormio.Open(
		postgres.New(postgres.Config{Conn: database.DB()}),
		&gormio.Config{
			DisableAutomaticPing:   true,
			SkipDefaultTransaction: true,
		},
	)
	if err != nil {
		return rollback(fmt.Errorf("dependencies: open GORM on PostgreSQL pool: %w", err))
	}
	gormDatabase, err := kgorm.Wrap(
		gormHandle,
		database.DB(),
		kgorm.Config{
			Owns:        false,
			System:      sqlConfig.Pool.System,
			Name:        sqlConfig.Pool.Name,
			MaxIdle:     sqlConfig.Pool.MaxIdle,
			MaxOpen:     sqlConfig.Pool.MaxOpen,
			MaxIdleTime: sqlConfig.Pool.MaxIdleTime,
			MaxLifetime: sqlConfig.Pool.MaxLifetime,
		},
		kgorm.WithTracerProvider(telemetry.TracerProvider()),
		kgorm.WithMeterProvider(telemetry.MeterProvider()),
	)
	if err != nil {
		return rollback(fmt.Errorf("dependencies: wrap GORM: %w", err))
	}
	resources.gorm = gormDatabase

	redisConfig, ok := loaded.Redis.Current()
	if !ok {
		return rollback(fmt.Errorf("dependencies: Redis config is unavailable"))
	}
	redisClient, err := kredis.Open(ctx, redisConfig, secrets)
	if err != nil {
		return rollback(fmt.Errorf("dependencies: build Redis: %w", err))
	}
	resources.redis = redisClient
	cache, err := kcache.FromClient(redisClient.Universal(), kcache.Config{
		Prefix: "channelcore",
		Owns:   false,
	})
	if err != nil {
		return rollback(fmt.Errorf("dependencies: build Redis cache: %w", err))
	}
	resources.cache = cache
	deliveryRouter := delivery.NewRouter()
	outboxConfig, ok := loaded.Outbox.Current()
	if !ok {
		return rollback(fmt.Errorf("dependencies: Outbox config is unavailable"))
	}
	outboxRuntime, err := koutbox.NewRuntime(
		outboxConfig,
		"outbox.delivery",
		loaded.Runtime.AppName+"-"+uuid.NewString(),
		database.DB(),
		deliveryRouter,
	)
	if err != nil {
		return rollback(fmt.Errorf("dependencies: build PostgreSQL Outbox: %w", err))
	}
	resources.outbox = outboxRuntime
	resources.delivery = deliveryRouter

	resources.components = []app.Component{
		app.ComponentFunc{
			ComponentName: "sql.primary",
			StartFunc:     database.Start,
			StopFunc:      database.Shutdown,
		},
		app.ComponentFunc{
			ComponentName: "gorm.primary",
			StartFunc:     gormDatabase.Start,
			StopFunc:      gormDatabase.Shutdown,
		},
		app.ComponentFunc{
			ComponentName: "redis.cache",
			StartFunc:     redisClient.Start,
			StopFunc:      redisClient.Shutdown,
		},
	}
	resources.statuses = []ops.RuntimeStatusRegistration{
		{Name: "primary", Kind: "sql", Provider: sqlStatus(database)},
		{Name: "primary", Kind: "gorm", Provider: kgorm.RuntimeStatus(gormDatabase)},
		{Name: "cache", Kind: "redis", Provider: redisStatus(redisClient)},
		{Name: "delivery", Kind: "outbox", Provider: outboxStatus(outboxRuntime.Dispatcher())},
	}
	resources.servers = append(resources.servers, outboxRuntime.Dispatcher())
	return resources, nil
}
