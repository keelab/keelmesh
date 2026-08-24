package database

import (
	"context"
	stdsql "database/sql"
	"fmt"

	ksql "github.com/keelab/contrib/data/sql"
)

// Transactional is implemented by Keelith contrib/data/sql.Database.
type Transactional interface {
	TransactionContext(
		context.Context,
		string,
		*stdsql.TxOptions,
		func(context.Context, *stdsql.Tx) error,
	) error
}

var _ Transactional = (*ksql.Database)(nil)

// Runner delegates transaction lifecycle, telemetry, rollback, commit, and
// panic handling to Keelith while keeping use cases independent of the pool.
type Runner struct {
	database Transactional
}

// NewRunner creates a borrowed transaction runner; it does not own or close
// the supplied Keelith database.
func NewRunner(database Transactional) (*Runner, error) {
	if database == nil {
		return nil, fmt.Errorf("database: transaction source is required")
	}
	return &Runner{database: database}, nil
}

// Run executes work in one Keelith-instrumented transaction using a stable,
// low-cardinality operation name.
func (runner *Runner) Run(
	ctx context.Context,
	operation string,
	options *stdsql.TxOptions,
	work func(context.Context, *stdsql.Tx) error,
) error {
	if runner == nil || runner.database == nil {
		return fmt.Errorf("database: transaction runner is not initialized")
	}
	return runner.database.TransactionContext(ctx, operation, options, work)
}
