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
	var deletedIDs []int
	listed := false

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			if listed {
				return nil, nil
			}
			listed = true
			return testRows(), nil
		},
		deleteValidationFn: func(_ context.Context, ids []int) error {
			deletedIDs = append(deletedIDs, ids...)
			return nil
		},
	}

	producer := func(_ context.Context, payload []byte, _ map[string]string) error {
		producedPayload = payload
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * testPollInterval)
		cancel()
	}()

	dks.Start(ctx)

	require.NotEmpty(t, producedPayload)
	require.Contains(t, string(producedPayload), "AAA")
	require.Contains(t, string(producedPayload), "BBB")
	require.Equal(t, []int{1, 2}, deletedIDs)
}

func TestDKS_EmptyDB(t *testing.T) {
	t.Parallel()

	producerCalled := false
	deleteCalled := false

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			return nil, nil
		},
		deleteValidationFn: func(_ context.Context, _ []int) error {
			deleteCalled = true
			return nil
		},
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

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

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			return nil, errors.New("db read failed")
		},
		deleteValidationFn: func(_ context.Context, _ []int) error {
			deleteCalled = true
			return nil
		},
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

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

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			return testRows(), nil
		},
		deleteValidationFn: func(_ context.Context, _ []int) error {
			deleteCalled = true
			return nil
		},
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		return errors.New("kafka down")
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

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

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			return testRows(), nil
		},
		deleteValidationFn: func(_ context.Context, _ []int) error {
			return errors.New("delete failed")
		},
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		producerCalled = true
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

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

	mock := &mockStoreClient{
		listValidationFn: func(_ context.Context, _ int) ([]store.ValidationMessageRow, error) {
			return nil, nil
		},
		deleteValidationFn: func(_ context.Context, _ []int) error {
			return nil
		},
	}

	producer := func(_ context.Context, _ []byte, _ map[string]string) error {
		return nil
	}

	dks := NewDatabaseKafkaSynchronizer(mock, producer, 10, testPollInterval)

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
