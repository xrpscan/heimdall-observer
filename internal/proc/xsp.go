package proc

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// StreamProcessor is an abstraction to read a stream of messages and process them efficiently in
// batches. It accepts a BulkOperation which dictates how each message is processed.
type StreamProcessor[T any] struct {
	streamName     string
	stream         <-chan T
	batchProc      *batchProcessor[T]
	autoFlushDelay time.Duration
}

// NewStreamProcessor creates a new [StreamProcessor] instance.
func NewStreamProcessor[T any](
	streamName string, stream <-chan T,
	bulkOp BulkOperation[T], maxBatchSize int, autoFlushDelay time.Duration,
) *StreamProcessor[T] {
	batchProc := newBatchProcessor(maxBatchSize, bulkOp)
	return &StreamProcessor[T]{
		streamName:     streamName,
		stream:         stream,
		batchProc:      batchProc,
		autoFlushDelay: autoFlushDelay,
	}
}

// Start reading the stream. This is a blocking call.
//
// It reads messages coming through the provided stream and processes them in batches.
func (s *StreamProcessor[T]) Start(ctx context.Context) {
	// Child logger with common fields set.
	clog := slog.With("name", s.streamName)

	// Ticker for periodic auto-flushing.
	ticker := time.NewTicker(s.autoFlushDelay)
	defer ticker.Stop()

	for {
		select {
		// If context has expired, break the infinite loop.
		case <-ctx.Done():
			clog.InfoContext(ctx, "successfully stopped reading the stream")
			return
		// Flush the batch periodically.
		case <-ticker.C:
			if count, err := s.batchProc.flush(ctx); err != nil {
				clog.ErrorContext(ctx, "failed to auto-flush batch", "error", err)
			} else {
				clog.InfoContext(ctx, "successfully auto-flushed messages", "count", count)
			}
		case item, open := <-s.stream:
			if !open {
				// This should be printed only once. In the next iteration, the ctx should be found
				// canceled, so the control never re-reaches here.
				clog.WarnContext(ctx, "stream is closed")
				continue
			}

			// Ticker should be reset for each flush call.
			ticker.Reset(s.autoFlushDelay)

			// Submit the item for processing. It will process only if the batch size has met.
			if count, err := s.batchProc.addItem(ctx, item); err != nil {
				clog.ErrorContext(ctx, "failed to process batch", "error", err)
			} else if count > 0 {
				clog.InfoContext(ctx, "successfully processed batch", "count", count)
			}
		}
	}
}

// Close implements the Closer interface of the registry package.
//
// Note that it does not unblock the Start call. The Start call is unblocked only when context
// passed to it expires.
func (s *StreamProcessor[T]) Close(ctx context.Context) error {
	count, err := s.batchProc.flush(ctx)
	if err != nil {
		return fmt.Errorf("failed to flush remaining messages: %w", err)
	}

	slog.InfoContext(ctx, "successfully flushed remaining messages",
		"count", count, "name", s.streamName)
	return nil
}
