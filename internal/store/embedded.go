package store

import (
	"context"
	"database/sql"
	"fmt"

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
	panic("unimplemented")
}

// DeleteValidationMessages implements [Client].
func (e *Embedded) DeleteValidationMessages(ctx context.Context, ids []int) error {
	panic("unimplemented")
}

// ListValidationMessages implements [Client].
func (e *Embedded) ListValidationMessages(ctx context.Context, limit int) ([]ValidationReceivedMessageRow, error) {
	panic("unimplemented")
}

// Close the database connection.
func (e *Embedded) Close(ctx context.Context) error {
	// TODO: Respect context.
	return e.db.Close()
}
