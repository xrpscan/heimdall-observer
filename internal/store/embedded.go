package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/shivanshkc/observer/pkg/rippled"

	_ "modernc.org/sqlite"
)

// Embedded implements Client as an embedded database.
type Embedded struct {
	db *sql.DB
}

// NewEmbedded returns a new Embedded instance.
func NewEmbedded(ctx context.Context, filePath string) (*Embedded, error) {
	db, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("error in sql.Open call: %w", err)
	}

	// Ping to ensure connection health.
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Embedded{db: db}, nil
}

// BulkInsertValidationMessages implements [Client].
func (e *Embedded) BulkInsertValidationMessages(ctx context.Context, messages []rippled.MessageValidationReceived) error {
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

	slog.InfoContext(ctx, "successfully inserted messages", "count", count)
	return nil
}

// DeleteValidationMessages implements [Client].
func (e *Embedded) DeleteValidationMessages(ctx context.Context, ids []int) error {
	panic("unimplemented")
}

// ListValidationMessages implements [Client].
func (e *Embedded) ListValidationMessages(ctx context.Context, limit int) ([]ValidationMessageRow, error) {
	panic("unimplemented")
}

// Close the database connection.
func (e *Embedded) Close(ctx context.Context) error {
	// TODO: Respect context.
	return e.db.Close()
}

func (e *Embedded) queryBulkInsertValidationMessages(
	messages []rippled.MessageValidationReceived,
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
