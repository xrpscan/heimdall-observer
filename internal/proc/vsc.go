package proc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/rippled"
)

// validationStreamBatchSize is the maxBatchSize value used to process messages from the
// validation stream.
const validationStreamBatchSize = 50

// ValidationStreamConsumer is an abstraction to consume a stream of validatioReceived messages
// from rippled and push them to an embedded database.
type ValidationStreamConsumer struct {
	embedded  store.Client
	stream    <-chan rippled.MessageValidationReceived
	batchProc *batchProcessor[rippled.MessageValidationReceived]
}

// NewValidationStreamConsumer creates a new [ValidationStreamConsumer] instance.
func NewValidationStreamConsumer(
	stream <-chan rippled.MessageValidationReceived,
	embedded store.Client,
) *ValidationStreamConsumer {
	// All validators broadcast their validation near-simultaneously after each ledger close.
	// So, messages arrive in bursts of size 100-150. We shouldn't be doing 100-150 database calls.
	// Instead, we'll do inserts in batches of size 50, which leads to 2-3 database calls per burst.
	//
	// TODO: Wrap the BulkInsertValidationMessages call into a retryable logic?
	batchProc := newBatchProcessor(validationStreamBatchSize, embedded.BulkInsertValidationMessages)
	return &ValidationStreamConsumer{embedded: embedded, stream: stream, batchProc: batchProc}
}

// Start consuming from the stream. This is a blocking call.
//
// It reads messages coming through the provided stream and inserts them into the provided database.
func (v *ValidationStreamConsumer) Start(ctx context.Context) {
	for {
		select {
		// If context has expired, break the infinite loop.
		case <-ctx.Done():
			slog.InfoContext(ctx, "successfully stopped validation stream consumption")
			return
		case item, open := <-v.stream:
			if !open {
				slog.WarnContext(ctx, "validation stream is closed, "+
					"context should be found expired in the next iteration")
				continue
			}

			// Submit the item for processing. It will process only if the batch size has met.
			if err := v.batchProc.addItem(ctx, item); err != nil {
				slog.ErrorContext(ctx, "failed to process a batch, MESSAGES WILL BE LOST", "error", err)
			}
		}
	}
}

// Close implements the Closer interface of the registry package.
//
// Note that it does not unblock the Start call. The Start call is unblocked only when context
// passed to it expires.
func (v *ValidationStreamConsumer) Close(ctx context.Context) error {
	if err := v.batchProc.flush(ctx); err != nil {
		return fmt.Errorf("failed to flush remaining messages: %w", err)
	}

	slog.InfoContext(ctx, "successfully flushed remaining messages to db")
	return nil
}
