package proc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/xrpscan/heimdall-observer/internal/store"
	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// ProducerFunc represents a Kafka producer.
type ProducerFunc func(context.Context, []byte, map[string]string) error

// DatabaseKafkaSynchronizer is an abstraction to poll the given database for records, produce them
// to Kafka using the given client, and then cleanup those records from the database.
type DatabaseKafkaSynchronizer struct {
	embedded     store.Client
	producer     ProducerFunc
	maxBatchSize int
	pollInterval time.Duration
}

// NewDatabaseKafkaSynchronizer returns a new instance of [DatabaseKafkaSynchronizer].
func NewDatabaseKafkaSynchronizer(
	embedded store.Client, producer ProducerFunc, maxBatchSize int, pollInterval time.Duration,
) *DatabaseKafkaSynchronizer {
	return &DatabaseKafkaSynchronizer{
		embedded:     embedded,
		producer:     producer,
		maxBatchSize: maxBatchSize,
		pollInterval: pollInterval,
	}
}

// Start synchronization. This is a blocking call.
func (d *DatabaseKafkaSynchronizer) Start(ctx context.Context) {
	// Ticker for polling the embedded DB.
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Poll database.
			rows, err := d.embedded.ListValidationMessages(ctx, d.maxBatchSize)
			if err != nil {
				slog.ErrorContext(ctx, "failed to list validation messages from db", "error", err)
				continue
			}

			// If no rows, do not produce empty slice to Kafka.
			count := len(rows)
			if count == 0 {
				slog.DebugContext(ctx, "no rows to process")
				continue
			}

			// Only the validation message is produced to Kafka, not the whole row.
			messages := make([]xrpld.MessageValidationReceived, len(rows))
			for i, row := range rows {
				messages[i] = row.Message
			}

			// Marshal messages for Kafka production.
			messagesBytes, err := json.Marshal(messages)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal validation messages", "error", err)
				continue
			}

			// Produce to Kafka.
			if err := d.producer(ctx, messagesBytes, map[string]string{}); err != nil {
				slog.ErrorContext(ctx, "failed to produce messages to kafka", "error", err)
				continue
			}
			slog.DebugContext(ctx, "successfully produced batch to kafka", "count", count)

			// Clean from database.
			if err := deleteValidationMessages(ctx, d.embedded, rows); err != nil {
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
func (d *DatabaseKafkaSynchronizer) Close(ctx context.Context) error {
	// Nothing to close yet.
	return nil
}

func deleteValidationMessages(
	ctx context.Context, embedded store.Client, messages []store.ValidationMessageRow,
) error {
	ids := make([]int, len(messages))
	for i, message := range messages {
		ids[i] = message.ID
	}

	return embedded.DeleteValidationMessages(ctx, ids)
}
