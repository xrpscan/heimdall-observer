package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"

	_ "modernc.org/sqlite"
)

// Embedded implements Client as an embedded database.
type Embedded struct {
	db *sql.DB
}

// NewEmbedded returns a new Embedded instance.
func NewEmbedded(ctx context.Context, filePath string) (*Embedded, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return nil, fmt.Errorf("failed to create parent directory for the database file: %w", err)
	}

	// busy_timeout(5000): Wait up to 5s for a lock before returning SQLITE_BUSY.
	// journal_mode(WAL): Allow concurrent reads and writes via write-ahead logging.
	dsn := fmt.Sprintf(`file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)`, filePath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("error in sql.Open call: %w", err)
	}

	// SQLite only supports one writer at a time. Limiting to one connection serializes
	// all writes and prevents SQLITE_BUSY from concurrent write attempts within this process.
	db.SetMaxOpenConns(1)

	// Ping to ensure connection health.
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Embedded{db: db}, nil
}

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

// Close the database connection.
func (e *Embedded) Close(ctx context.Context) error {
	// TODO: Respect context.
	return e.db.Close()
}

func (e *Embedded) queryBulkInsertValidationMessages(
	messages []xrpld.MessageValidationReceived,
) (string, []any, error) {
	var values string
	args := make([]any, len(messages))

	for i, message := range messages {
		// Message needs to be marshalled since its type in the database is text.
		messageBytes, err := json.Marshal(message)
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal message: %w", err)
		}

		values += fmt.Sprintf(`($%d), `, i+1)
		args[i] = string(messageBytes)
	}

	// Remove trailing comma-space from the earlier string-building.
	values = strings.TrimSuffix(values, ", ")
	return `INSERT INTO validations (message) VALUES ` + values + ";", args, nil
}

func (e *Embedded) queryDeleteValidationMessages(ids []int) (string, []any) {
	args := make([]any, len(ids))
	arrString := "("
	for i := range ids {
		args[i] = ids[i]
		arrString += fmt.Sprintf(`$%d, `, i+1)
	}
	arrString = strings.TrimSuffix(arrString, ", ")
	arrString += ")"

	return `DELETE FROM validations WHERE id IN ` + arrString, args
}
