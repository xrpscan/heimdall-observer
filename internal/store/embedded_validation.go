package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"
)

// BulkInsertValidationMessages implements [Client].
func (e *Embedded) BulkInsertValidationMessages(ctx context.Context, messages []xrpld.MessageValidationReceived) error {
	// Form query.
	query, args, err := e.queryBulkInsertValidationMessages(messages)
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

// DeleteValidationMessages implements [Client].
func (e *Embedded) DeleteValidationMessages(ctx context.Context, ids []int) error {
	query, args := e.queryDeleteValidationMessages(ids)

	// Execute query.
	result, err := e.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	aff, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected row count: %w", err)
	}

	// Verify that expected number of rows were deleted.
	if int(aff) != len(ids) {
		return fmt.Errorf("unexpected number of rows were deleted: %d, expected: %d", aff, len(ids))
	}

	return nil
}

// ListValidationMessages implements [Client].
func (e *Embedded) ListValidationMessages(ctx context.Context, limit int) ([]ValidationMessageRow, error) {
	query, args :=
		`SELECT id, message, created_at FROM validations ORDER BY created_at LIMIT $1`,
		[]any{limit}

	// Execute query.
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error in query execution: %w", err)
	}
	// Cleanup.
	defer func() { _ = rows.Close() }()

	// This will store the results.
	var messages []ValidationMessageRow

	// Loop over rows for decoding data.
	for rows.Next() {
		var m ValidationMessageRow
		var textContent string

		if err := rows.Scan(&m.ID, &textContent, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert from plain text into structure.
		if err := json.Unmarshal([]byte(textContent), &m.Message); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message content into struct: %w", err)
		}

		// Collect.
		messages = append(messages, m)
	}

	// If there's an error while scanning (not returned by rows.Scan), rows.Next may return false
	// and the loop may end early. This error is caught by rows.Err here.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err returned error: %w", err)
	}

	return messages, nil
}

func (e *Embedded) queryBulkInsertValidationMessages(
	messages []xrpld.MessageValidationReceived,
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
	return `INSERT INTO validations (message) VALUES ` + values + ";", args, nil
}

func (e *Embedded) queryDeleteValidationMessages(ids []int) (string, []any) {
	args := make([]any, len(ids))

	var builder strings.Builder
	builder.Grow(len(ids) * 5) // Reasonable buffer pre-allocation.
	builder.WriteString("(")

	for i := range ids {
		args[i] = ids[i]
		fmt.Fprintf(&builder, `$%d, `, i+1)
	}

	arrString := strings.TrimSuffix(builder.String(), ", ")
	arrString += ")"

	return `DELETE FROM validations WHERE id IN ` + arrString, args
}
