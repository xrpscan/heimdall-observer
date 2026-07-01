package store

import (
	"context"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// Client for the application's storage layer.
type Client interface {
	// BulkInsertValidationMessages allows inserting multiple validation messages into the DB.
	// A bulk insert helps because the caller can insert in batches if their message influx is high.
	BulkInsertValidationMessages(
		ctx context.Context, messages []xrpld.MessageValidationReceived,
	) error

	// ListValidationMessages returns validation messages, oldest first. The limit parameter
	// controls the max number of messages that can be returned.
	ListValidationMessages(ctx context.Context, limit int) ([]ValidationMessageRow, error)

	// DeleteValidationMessages deletes the validation messages whose ID matches one of the given ids.
	DeleteValidationMessages(ctx context.Context, ids []int) error
}
