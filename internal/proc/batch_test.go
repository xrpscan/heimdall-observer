package proc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchProcessor_BelowThreshold(t *testing.T) {
	t.Parallel()

	called := false
	bp := newBatchProcessor(5, func(_ context.Context, _ []int) error {
		called = true
		return nil
	})

	for i := range 4 {
		require.NoError(t, bp.addItem(context.Background(), i))
	}

	require.False(t, called)
}

func TestBatchProcessor_AtThreshold(t *testing.T) {
	t.Parallel()

	var received []int
	bp := newBatchProcessor(3, func(_ context.Context, items []int) error {
		received = append(received, items...)
		return nil
	})

	for i := range 3 {
		require.NoError(t, bp.addItem(context.Background(), i))
	}

	require.Equal(t, []int{0, 1, 2}, received)
}

func TestBatchProcessor_ManualFlush(t *testing.T) {
	t.Parallel()

	var received []int
	bp := newBatchProcessor(10, func(_ context.Context, items []int) error {
		received = append(received, items...)
		return nil
	})

	for i := range 3 {
		require.NoError(t, bp.addItem(context.Background(), i))
	}

	require.NoError(t, bp.flush(context.Background()))
	require.Equal(t, []int{0, 1, 2}, received)
}

func TestBatchProcessor_FlushEmpty(t *testing.T) {
	t.Parallel()

	called := false
	bp := newBatchProcessor(5, func(_ context.Context, _ []int) error {
		called = true
		return nil
	})

	require.NoError(t, bp.flush(context.Background()))
	require.False(t, called)
}

func TestBatchProcessor_OperationError(t *testing.T) {
	t.Parallel()

	opErr := errors.New("db failure")
	bp := newBatchProcessor(2, func(_ context.Context, _ []int) error {
		return opErr
	})

	require.NoError(t, bp.addItem(context.Background(), 1))

	err := bp.addItem(context.Background(), 2)
	require.ErrorIs(t, err, opErr)
}

func TestBatchProcessor_OperationErrorOnFlush(t *testing.T) {
	t.Parallel()

	opErr := errors.New("db failure")
	bp := newBatchProcessor(10, func(_ context.Context, _ []int) error {
		return opErr
	})

	require.NoError(t, bp.addItem(context.Background(), 1))

	err := bp.flush(context.Background())
	require.ErrorIs(t, err, opErr)
}

func TestBatchProcessor_MultipleBatches(t *testing.T) {
	t.Parallel()

	var calls [][]int
	bp := newBatchProcessor(3, func(_ context.Context, items []int) error {
		calls = append(calls, append([]int{}, items...))
		return nil
	})

	for i := range 7 {
		require.NoError(t, bp.addItem(context.Background(), i))
	}

	require.Len(t, calls, 2)
	require.Equal(t, []int{0, 1, 2}, calls[0])
	require.Equal(t, []int{3, 4, 5}, calls[1])

	require.NoError(t, bp.flush(context.Background()))
	require.Len(t, calls, 3)
	require.Equal(t, []int{6}, calls[2])
}

func TestBatchProcessor_FlushClearsItems(t *testing.T) {
	t.Parallel()

	callCount := 0
	bp := newBatchProcessor(10, func(_ context.Context, _ []int) error {
		callCount++
		return nil
	})

	require.NoError(t, bp.addItem(context.Background(), 1))
	require.NoError(t, bp.flush(context.Background()))
	require.NoError(t, bp.flush(context.Background()))

	require.Equal(t, 1, callCount)
}
