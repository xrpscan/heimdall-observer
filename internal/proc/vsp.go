package proc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/rippled"
)

// ValidationStreamProcessor is an abstraction to read a stream of validationReceived messages
// from rippled and push them to an embedded database.
type ValidationStreamProcessor struct {
	embedded       store.Client
	stream         <-chan rippled.MessageValidationReceived
	batchProc      *batchProcessor[rippled.MessageValidationReceived]
	autoFlushDelay time.Duration
}

// NewValidationStreamProcessor creates a new [ValidationStreamProcessor] instance.
func NewValidationStreamProcessor(
	stream <-chan rippled.MessageValidationReceived,
	embedded store.Client,
	maxBatchSize int, autoFlushDelay time.Duration,
) *ValidationStreamProcessor {
	// All validators broadcast their validation near-simultaneously after each ledger close.
	// So, messages arrive in bursts of size 100-150. We shouldn't be doing 100-150 database calls.
	// Instead, we'll do inserts in batches of size 50, which leads to 2-3 database calls per burst.
	//
	// TODO: Wrap the BulkInsertValidationMessages call into a retryable logic?
	batchProc := newBatchProcessor(maxBatchSize, embedded.BulkInsertValidationMessages)
	return &ValidationStreamProcessor{
		embedded:       embedded,
		stream:         stream,
		batchProc:      batchProc,
		autoFlushDelay: autoFlushDelay,
	}
}

// Start reading the stream. This is a blocking call.
//
// It reads messages coming through the provided stream and inserts them into the provided database.
func (v *ValidationStreamProcessor) Start(ctx context.Context) {
	// Ticker for periodic auto-flushing.
	ticker := time.NewTicker(v.autoFlushDelay)
	defer ticker.Stop()

	for {
		select {
		// If context has expired, break the infinite loop.
		case <-ctx.Done():
			slog.InfoContext(ctx, "successfully stopped reading the validation stream")
			return
		// Flush the batch periodically.
		case <-ticker.C:
			if count, err := v.batchProc.flush(ctx); err != nil {
				slog.ErrorContext(ctx, "failed to auto-flush batch to db", "error", err)
			} else {
				slog.InfoContext(ctx, "successfully auto-flushed messages to db", "count", count)
			}
		case item, open := <-v.stream:
			if !open {
				// This should be printed only once. In the next iteration, the ctx should be found
				// canceled, so the control never re-reaches here.
				slog.WarnContext(ctx, "validation stream is closed")
				continue
			}

			// Ticker should be reset for each flush call.
			ticker.Reset(v.autoFlushDelay)

			// Submit the item for processing. It will process only if the batch size has met.
			if count, err := v.batchProc.addItem(ctx, item); err != nil {
				slog.ErrorContext(ctx, "failed to store message batch to db", "error", err)
			} else if count > 0 {
				slog.DebugContext(ctx, "successfully stored message batch to db", "count", count)
			}
		}
	}
}

// Close implements the Closer interface of the registry package.
//
// Note that it does not unblock the Start call. The Start call is unblocked only when context
// passed to it expires.
func (v *ValidationStreamProcessor) Close(ctx context.Context) error {
	count, err := v.batchProc.flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush remaining messages to db: %w", err)
	}

	slog.InfoContext(ctx, "successfully flushed remaining messages to db", "count", count)
	return nil
}
