package proc

import (
	"context"
	"log/slog"

	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/rippled"
)

// ConsumeValidationStream reads messages coming through the provided stream and inserts them
// reliably into the provided database.
func ConsumeValidationStream(ctx context.Context,
	stream <-chan rippled.MessageValidationReceived,
	embedded store.Client,
) {
	// All validators broadcast their validation near-simultaneously after each ledger close.
	// So, messages arrive in bursts of size 100-150. We shouldn't be doing 100-150 database calls.
	// Instead, we'll do inserts in batches of size 50, which leads to 2-3 database calls per burst.
	//
	// TODO: Wrap the BulkInsertValidationMessages call into a retryable logic?
	batchProc := newBatchProcessor(50, embedded.BulkInsertValidationMessages)

	// Flush any remaining messages before returning.
	defer func() {
		if err := batchProc.flush(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to flush remaining messages", "error", err)
		} else {
			slog.InfoContext(ctx, "successfully flushed remaining messages to db")
		}
	}()

	for {
		// If context has expired, break the infinite loop.
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "successfully stopped validation stream consumption")
			return
		default:
		}

		item, open := <-stream
		if !open {
			slog.WarnContext(ctx, "validation stream is closed, "+
				"context should be found expired in the next iteration")
			continue
		}

		// Submit the item for processing. It will process only if the batch size has met.
		if err := batchProc.addItem(ctx, item); err != nil {
			slog.ErrorContext(ctx, "failed to process a batch, MESSAGES WILL BE LOST", "error", err)
		}
	}
}
