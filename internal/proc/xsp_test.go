package proc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testBatchSize      = 5
	testAutoFlushDelay = time.Minute
)

func TestStreamProcessor_ConsumesMessages(t *testing.T) {
	t.Parallel()

	var received []string
	bulkOp := func(_ context.Context, msgs []string) error {
		received = append(received, msgs...)
		return nil
	}

	stream := make(chan string, 100)
	sp := NewStreamProcessor("test", stream, bulkOp, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	for range testBatchSize {
		stream <- "A"
	}

	done := make(chan struct{})
	go func() {
		sp.Start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	require.Len(t, received, testBatchSize)
}

func TestStreamProcessor_StartExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	bulkOp := func(_ context.Context, _ []string) error {
		return nil
	}

	stream := make(chan string)
	sp := NewStreamProcessor("test", stream, bulkOp, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sp.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancellation")
	}
}

func TestStreamProcessor_CloseFlushesRemaining(t *testing.T) {
	t.Parallel()

	var received []string
	bulkOp := func(_ context.Context, msgs []string) error {
		received = append(received, msgs...)
		return nil
	}

	stream := make(chan string, 100)
	sp := NewStreamProcessor("test", stream, bulkOp, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	for range testBatchSize - 1 {
		stream <- "X"
	}

	done := make(chan struct{})
	go func() {
		sp.Start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	require.Empty(t, received)

	cancel()
	<-done

	require.NoError(t, sp.Close(context.Background()))
	require.Len(t, received, testBatchSize-1)
}

func TestStreamProcessor_CloseReturnsError(t *testing.T) {
	t.Parallel()

	bulkOp := func(_ context.Context, _ []string) error {
		return errors.New("db down")
	}

	stream := make(chan string, 100)
	sp := NewStreamProcessor("test", stream, bulkOp, testBatchSize, testAutoFlushDelay)

	ctx, cancel := context.WithCancel(context.Background())

	stream <- "Y"

	done := make(chan struct{})
	go func() {
		sp.Start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	err := sp.Close(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "db down")
}

func TestStreamProcessor_AutoFlush(t *testing.T) {
	t.Parallel()

	var received []string
	bulkOp := func(_ context.Context, msgs []string) error {
		received = append(received, msgs...)
		return nil
	}

	stream := make(chan string, 100)
	sp := NewStreamProcessor("test", stream, bulkOp, 1000, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	for range 3 {
		stream <- "Z"
	}

	done := make(chan struct{})
	go func() {
		sp.Start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(stream) == 0 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(2 * sp.autoFlushDelay)

	cancel()
	<-done

	require.Len(t, received, 3)
}
