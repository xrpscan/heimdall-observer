package proc

import (
	"context"

	"github.com/shivanshkc/observer/internal/store"
)

// DatabaseKafkaSynchronizer is an abstraction to poll the given database for records, produce them
// to Kafka using the given client, and then cleanup those records from the database.
type DatabaseKafkaSynchronizer struct{}

// NewDatabaseKafkaSynchronizer returns a new instance of [DatabaseKafkaSynchronizer].
func NewDatabaseKafkaSynchronizer(embedded store.Client, producer any) *DatabaseKafkaSynchronizer {
	return nil
}

// Start synchronization. This is a blocking call.
func (d *DatabaseKafkaSynchronizer) Start(ctx context.Context) {}

// Close implements the Closer interface of the registry package.
//
// Note that it does not unblock the Start call. The Start call is unblocked only when context
// passed to it expires.
func (d *DatabaseKafkaSynchronizer) Close(ctx context.Context) error {
	return nil
}
