package proc

import (
	"context"

	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/rippled"
)

// mockStoreClient implements store.Client for testing.
type mockStoreClient struct {
	bulkInsertFn func(ctx context.Context, messages []rippled.MessageValidationReceived) error
}

func (m *mockStoreClient) BulkInsertValidationMessages(ctx context.Context, messages []rippled.MessageValidationReceived) error {
	return m.bulkInsertFn(ctx, messages)
}

func (m *mockStoreClient) ListValidationMessages(ctx context.Context, limit int) ([]store.ValidationMessageRow, error) {
	panic("unimplemented")
}

func (m *mockStoreClient) DeleteValidationMessages(ctx context.Context, ids []int) error {
	panic("unimplemented")
}
