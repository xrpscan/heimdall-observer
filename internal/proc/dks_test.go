package proc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xrpscan/heimdall-observer/internal/store"
	"github.com/xrpscan/heimdall-observer/pkg/xrpld"

	"github.com/stretchr/testify/require"
)

const testPollInterval = 50 * time.Millisecond

func testRows() []store.ValidationMessageRow {
	return []store.ValidationMessageRow{
		{ID: 1, Message: xrpld.MessageValidationReceived{LedgerHash: "AAA"}},
		{ID: 2, Message: xrpld.MessageValidationReceived{LedgerHash: "BBB"}},
	}
}

func TestDKS_HappyPath(t *testing.T) {
	t.Parallel()

	var producedPayload []byte
	var deletedRows []store.ValidationMessageRow
	listed := false

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		if listed {
			return nil, nil
		}
		listed = true
		return testRows(), nil
	}

	deleteFn := func(_ context.Context, rows []store.ValidationMessageRow) error {
		deletedRows = append(deletedRows, rows...)
		return nil
	}

	producer := func(_ context.Context, payload []byte, _ map[string]string) error {
		producedPayload = payload
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.NotEmpty(t, producedPayload)
	require.Contains(t, string(producedPayload), "AAA")
	require.Contains(t, string(producedPayload), "BBB")
	require.Len(t, deletedRows, 2)
	require.Equal(t, 1, deletedRows[0].ID)
	require.Equal(t, 2, deletedRows[1].ID)
}

func TestDKS_EmptyDB(t *testing.T) {
	t.Parallel()

	producerCalled := false
	deleteCalled := false

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		return nil, nil
	}

	deleteFn := func(_ context.Context, _ []store.ValidationMessageRow) error {
		deleteCalled = true
		return nil
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.False(t, producerCalled)
	require.False(t, deleteCalled)
}

func TestDKS_ListError(t *testing.T) {
	t.Parallel()

	producerCalled := false
	deleteCalled := false

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		return nil, errors.New("db read failed")
	}

	deleteFn := func(_ context.Context, _ []store.ValidationMessageRow) error {
		deleteCalled = true
		return nil
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.False(t, producerCalled)
	require.False(t, deleteCalled)
}

func TestDKS_ProduceError(t *testing.T) {
	t.Parallel()

	deleteCalled := false

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		return testRows(), nil
	}

	deleteFn := func(_ context.Context, _ []store.ValidationMessageRow) error {
		deleteCalled = true
		return nil
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		return errors.New("kafka down")
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.False(t, deleteCalled)
}

func TestDKS_DeleteError(t *testing.T) {
	t.Parallel()

	producerCalled := false

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		return testRows(), nil
	}

	deleteFn := func(_ context.Context, _ []store.ValidationMessageRow) error {
		return errors.New("delete failed")
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.True(t, producerCalled)
}

func TestDKS_StartExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	pollFn := func(_ context.Context) ([]store.ValidationMessageRow, error) {
		return nil, nil
	}

	deleteFn := func(_ context.Context, _ []store.ValidationMessageRow) error {
		return nil
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(pollFn, testPollInterval, deleteFn, producer)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		dks.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not exit after context cancellation")
	}
}
