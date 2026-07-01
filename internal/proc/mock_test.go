package proc

import (
	"context"

	"github.com/xrpscan/heimdall-observer/internal/store"
	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// mockStoreClient implements store.Client for testing.
type mockStoreClient struct {
	bulkInsertFn func(ctx context.Context, messages []xrpld.MessageValidationReceived) error
	listFn       func(ctx context.Context, limit int) ([]store.ValidationMessageRow, error)
	deleteFn     func(ctx context.Context, ids []int) error
}

func (m *mockStoreClient) BulkInsertValidationMessages(ctx context.Context, messages []xrpld.MessageValidationReceived) error {
	return m.bulkInsertFn(ctx, messages)
}

func (m *mockStoreClient) ListValidationMessages(ctx context.Context, limit int) ([]store.ValidationMessageRow, error) {
	return m.listFn(ctx, limit)
}

func (m *mockStoreClient) DeleteValidationMessages(ctx context.Context, ids []int) error {
	return m.deleteFn(ctx, ids)
}
