package proc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"

	"github.com/stretchr/testify/require"
)

const (
	testBatchSize      = 5
	testAutoFlushDelay = time.Minute
)

func TestVSP_ConsumesMessages(t *testing.T) {
	t.Parallel()

	var received []xrpld.MessageValidationReceived
	mock := &mockStoreClient{
		bulkInsertFn: func(_ context.Context, msgs []xrpld.MessageValidationReceived) error {
			received = append(received, msgs...)
			return nil
		},
	}

	stream := make(chan xrpld.MessageValidationReceived, 100)
	vsp := NewValidationStreamProcessor(stream, mock, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	// Send enough messages to trigger a batch flush.
	for range testBatchSize {
		stream <- xrpld.MessageValidationReceived{LedgerHash: "A"}
	}

	done := make(chan struct{})
	go func() {
		vsp.Start(ctx)
		close(done)
	}()

	// Wait for the stream to be consumed before canceling the context.
	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	require.Len(t, received, testBatchSize)
}

func TestVSP_StartExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	mock := &mockStoreClient{
		bulkInsertFn: func(_ context.Context, _ []xrpld.MessageValidationReceived) error {
			return nil
		},
	}

	stream := make(chan xrpld.MessageValidationReceived)
	vsp := NewValidationStreamProcessor(stream, mock, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		vsp.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancellation")
	}
}

func TestVSP_CloseFlushesRemaining(t *testing.T) {
	t.Parallel()

	var received []xrpld.MessageValidationReceived
	mock := &mockStoreClient{
		bulkInsertFn: func(_ context.Context, msgs []xrpld.MessageValidationReceived) error {
			received = append(received, msgs...)
			return nil
		},
	}

	stream := make(chan xrpld.MessageValidationReceived, 100)
	vsp := NewValidationStreamProcessor(stream, mock, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	// Send fewer than threshold so no auto-flush happens.
	for range testBatchSize - 1 {
		stream <- xrpld.MessageValidationReceived{LedgerHash: "X"}
	}

	done := make(chan struct{})
	go func() {
		vsp.Start(ctx)
		close(done)
	}()

	// Wait for messages to be consumed from the channel.
	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	// No flush yet — below threshold.
	require.Empty(t, received)

	cancel()
	<-done

	// Close flushes the remaining items.
	require.NoError(t, vsp.Close(context.Background()))
	require.Len(t, received, testBatchSize-1)
}

func TestVSP_CloseReturnsError(t *testing.T) {
	t.Parallel()

	mock := &mockStoreClient{
		bulkInsertFn: func(_ context.Context, _ []xrpld.MessageValidationReceived) error {
			return errors.New("db down")
		},
	}

	stream := make(chan xrpld.MessageValidationReceived, 100)
	vsp := NewValidationStreamProcessor(stream, mock, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	stream <- xrpld.MessageValidationReceived{LedgerHash: "Y"}

	done := make(chan struct{})
	go func() {
		vsp.Start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	err := vsp.Close(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "db down")
}

func TestVSP_AutoFlush(t *testing.T) {
	t.Parallel()

	var received []xrpld.MessageValidationReceived
	mock := &mockStoreClient{
		bulkInsertFn: func(_ context.Context, msgs []xrpld.MessageValidationReceived) error {
			received = append(received, msgs...)
			return nil
		},
	}

	stream := make(chan xrpld.MessageValidationReceived, 100)
	// High batch size so threshold is never hit; short auto-flush delay.
	vsp := NewValidationStreamProcessor(stream, mock, 1000, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	for range 3 {
		stream <- xrpld.MessageValidationReceived{LedgerHash: "Z"}
	}

	done := make(chan struct{})
	go func() {
		vsp.Start(ctx)
		close(done)
	}()

	// Wait for stream to drain.
	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)
	// Wait for auto-flush to happen.
	time.Sleep(2 * vsp.autoFlushDelay)

	// Signal Start to return and wait for it.
	cancel()
	<-done

	// After Start returns, received is safe to read.
	require.Len(t, received, 3)
}
