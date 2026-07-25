package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// BulkInsertLedgerMessages implements [Client].
func (e *Embedded) BulkInsertLedgerMessages(
	ctx context.Context, messages []xrpld.MessageLedgerClosed,
) error {
	// Form query.
	query, args, err := e.queryBulkInsertLedgerMessages(messages)
	if err != nil {
		return fmt.Errorf("failed to form query and args: %w", err)
	}

	// Execute query.
	result, err := e.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error in query execution: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to obtain affected row count: %w", err)
	}

	// Verify that expected number of rows were inserted.
	if int(count) != len(messages) {
		return fmt.Errorf("unexpected number of rows were inserted: %d, expected: %d", count, len(messages))
	}

	return nil
}

func (e *Embedded) queryBulkInsertLedgerMessages(
	messages []xrpld.MessageLedgerClosed,
) (string, []any, error) {
	args := make([]any, len(messages))

	var valueBuilder strings.Builder
	valueBuilder.Grow(len(messages) * 7) // Reasonable buffer pre-allocation.

	for i, message := range messages {
		// Message needs to be marshalled since its type in the database is text.
		messageBytes, err := json.Marshal(message)
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal message: %w", err)
		}

		fmt.Fprintf(&valueBuilder, `($%d), `, i+1)
		args[i] = string(messageBytes)
	}

	// Remove trailing comma-space from the earlier string-building.
	values := strings.TrimSuffix(valueBuilder.String(), ", ")
	return `INSERT INTO ledger (message) VALUES ` + values + ";", args, nil
}
