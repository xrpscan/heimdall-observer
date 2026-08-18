package proc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// PollFunc represents an operation that fetches a list of records.
type PollFunc[T any] func(context.Context) ([]T, error)

// DeleteFunc represents an operation that deletes the given records.
type DeleteFunc[T any] func(context.Context, []T) error

// ProducerFunc represents a Kafka producer.
type ProducerFunc func(context.Context, []byte, map[string]string) error

// DatabaseKafkaSynchronizer is an abstraction to poll the given database for records, produce them
// to Kafka using the given client, and then cleanup those records from the database.
type DatabaseKafkaSynchronizer[T any] struct {
	pollFunc     PollFunc[T]
	pollInterval time.Duration
	deleteFunc   DeleteFunc[T]
	producer     ProducerFunc
}

// NewDatabaseKafkaSynchronizer returns a new instance of [DatabaseKafkaSynchronizer].
func NewDatabaseKafkaSynchronizer[T any](
	pollFunc PollFunc[T], pollInterval time.Duration,
	deleteFunc DeleteFunc[T], producer ProducerFunc,
) *DatabaseKafkaSynchronizer[T] {
	return &DatabaseKafkaSynchronizer[T]{
		pollFunc:     pollFunc,
		pollInterval: pollInterval,
		deleteFunc:   deleteFunc,
		producer:     producer,
	}
}

// Start synchronization. This is a blocking call.
func (d *DatabaseKafkaSynchronizer[T]) Start(ctx context.Context) {
	// Ticker for polling the embedded DB.
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Poll database.
			rows, err := d.pollFunc(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "failed to poll for records", "error", err)
				continue
			}

			// If no rows, do not produce empty slice to Kafka.
			count := len(rows)
			if count == 0 {
				slog.DebugContext(ctx, "no rows to process")
				continue
			}

			// Marshal messages for Kafka production.
			messagesBytes, err := json.Marshal(rows)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal records", "error", err)
				continue
			}

			// Produce to Kafka.
			if err := d.producer(ctx, messagesBytes, map[string]string{}); err != nil {
				slog.ErrorContext(ctx, "failed to produce messages to kafka", "error", err)
				continue
			}
			slog.InfoContext(ctx, "successfully produced batch to kafka", "count", count)

			// Clean from database.
			if err := d.deleteFunc(ctx, rows); err != nil {
				slog.ErrorContext(ctx, "failed to delete produced messages from db", "error", err)
				continue
			}
			slog.DebugContext(ctx, "successfully deleted batch from db", "count", count)
		}
	}
}

// Close implements the Closer interface of the registry package.
//
// Note that it does not unblock the Start call. The Start call is unblocked only when context
// passed to it expires.
func (d *DatabaseKafkaSynchronizer[T]) Close(ctx context.Context) error {
	// Nothing to close yet.
	return nil
}
