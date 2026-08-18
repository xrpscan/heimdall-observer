package proc

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// BulkOperation represents any operation that accepts a collection of items to process.
type BulkOperation[T any] func(context.Context, []T) error

// batchProcessor is an abstraction over a bulkOperation to achieve efficient batch processing.
//
// If you have a stream of data that you need to process, calling the bulkOperation for each new
// data item does not make sense. The batchProcessor collects the incoming data items and processes
// them only when their count reaches a threshold.
//
// Note that batchProcessor does not flush events periodically. So, if the source stream stops and
// the batch has not reached its threshold size, it will not be flushed automatically. Caller can
// flush it manually using the flush method.
type batchProcessor[T any] struct {
	maxBatchSize int
	itemsMutex   sync.RWMutex
	items        []T
	operation    BulkOperation[T]
}

// newBatchProcessor returns a new batchProcessor instance.
func newBatchProcessor[T any](maxBatchSize int, operation BulkOperation[T]) *batchProcessor[T] {
	return &batchProcessor[T]{
		maxBatchSize: maxBatchSize,
		itemsMutex:   sync.RWMutex{},
		items:        []T{},
		operation:    operation,
	}
}

// addItem adds a new item to the batch.
// This may lead to a flush call if the batch size has crossed the thershold, otherwise not.
//
// If a flush call is triggered, the exact number of items flushed are returned. Otherwise, zero
// is returned.
func (b *batchProcessor[T]) addItem(ctx context.Context, item T) (int, error) {
	b.itemsMutex.Lock()
	b.items = append(b.items, item)
	clone := slices.Clone(b.items)
	b.itemsMutex.Unlock()

	// Check if threshold is reached.
	if len(clone) < b.maxBatchSize {
		return 0, nil
	}

	// Threshold has reached. Flush items.
	return b.flush(ctx)
}

// flush items by calling the bulkOperation. It returns the exact number of items flushed.
func (b *batchProcessor[T]) flush(ctx context.Context) (int, error) {
	b.itemsMutex.Lock()
	defer b.itemsMutex.Unlock()

	count := len(b.items)
	if count == 0 {
		return 0, nil
	}

	if err := b.operation(ctx, b.items); err != nil {
		return 0, fmt.Errorf("error in bulk operation: %w", err)
	}

	b.items = nil
	return count, nil
}
