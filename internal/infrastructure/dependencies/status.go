package dependencies

import (
	"context"

	ksql "github.com/keelab/contrib/data/sql"
	kredis "github.com/keelab/contrib/redis"
	"github.com/keelab/keelith/ops"
	koutbox "github.com/keelab/keelith/outbox"
)

func lifecycleState(started, closed bool) string {
	switch {
	case closed:
		return "stopped"
	case started:
		return "running"
	default:
		return "configured"
	}
}

func redisStatus(client *kredis.Client) ops.RuntimeStatusProvider {
	return func(context.Context) (ops.RuntimeStatus, error) {
		description := client.Description()
		return ops.RuntimeStatus{
			State:        lifecycleState(description.Started, description.Closed),
			Ready:        description.Started && !description.Closed,
			Degraded:     description.HealthFailures > 0,
			Capabilities: []string{"health-check", "shared-client"},
		}, nil
	}

}
func sqlStatus(database *ksql.Database) ops.RuntimeStatusProvider {
	return func(context.Context) (ops.RuntimeStatus, error) {
		description := database.Description()
		return ops.RuntimeStatus{
			State:        lifecycleState(description.Started, description.Closed),
			Ready:        description.Started && !description.Closed,
			Degraded:     description.HealthFailures > 0,
			Active:       description.InUse,
			Capabilities: []string{"connection-pool", "health-check", "hot-pool-limits"},
		}, nil
	}
}

type outboxStatusSource interface {
	Description() koutbox.Description
}

func outboxStatus(dispatcher outboxStatusSource) ops.RuntimeStatusProvider {
	return func(context.Context) (ops.RuntimeStatus, error) {
		description := dispatcher.Description()
		return ops.RuntimeStatus{
			State: outboxLifecycleState(description.Running, description.Finished),
			Ready: description.Running && !description.Failed,
			Degraded: description.Failed ||
				description.ConsecutiveRepositoryFailures > 0 ||
				description.ConsecutivePublisherFailures > 0,
			Active: description.InFlight,
			Counters: []ops.RuntimeCounter{
				{Name: "claimed", Value: description.Claimed},
				{Name: "published", Value: description.Published},
				{Name: "publisher_failures", Value: description.PublisherFailures},
				{Name: "repository_failures", Value: description.RepositoryFailures},
				{Name: "rescheduled", Value: description.Rescheduled},
				{Name: "terminal", Value: description.Terminal},
			},
			Capabilities: []string{"bounded-retry", "lease-claim", "terminal-state", "transactional-enqueue"},
		}, nil
	}
}

func outboxLifecycleState(running, finished bool) string {
	switch {
	case finished:
		return "stopped"
	case running:
		return "running"
	default:
		return "configured"
	}
}
