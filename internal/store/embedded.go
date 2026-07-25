package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

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

// Close the database connection.
func (e *Embedded) Close(ctx context.Context) error {
	// TODO: Respect context.
	return e.db.Close()
}
