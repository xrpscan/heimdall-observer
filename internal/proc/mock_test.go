package proc

import (
	"context"

	"github.com/xrpscan/heimdall-observer/internal/store"
	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// mockStoreClient implements store.Client for testing.
type mockStoreClient struct {
	bulkInsertValidationFn func(ctx context.Context, messages []xrpld.MessageValidationReceived) error
	bulkInsertLedgerFn     func(ctx context.Context, messages []xrpld.MessageLedgerClosed) error
	listValidationFn       func(ctx context.Context, limit int) ([]store.ValidationMessageRow, error)
	deleteValidationFn     func(ctx context.Context, ids []int) error
}

func (m *mockStoreClient) BulkInsertValidationMessages(ctx context.Context, messages []xrpld.MessageValidationReceived) error {
	return m.bulkInsertValidationFn(ctx, messages)
}

func (m *mockStoreClient) BulkInsertLedgerMessages(ctx context.Context, messages []xrpld.MessageLedgerClosed) error {
	return m.bulkInsertLedgerFn(ctx, messages)
}

func (m *mockStoreClient) ListValidationMessages(ctx context.Context, limit int) ([]store.ValidationMessageRow, error) {
	return m.listValidationFn(ctx, limit)
}

func (m *mockStoreClient) DeleteValidationMessages(ctx context.Context, ids []int) error {
	return m.deleteValidationFn(ctx, ids)
}
