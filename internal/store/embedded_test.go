package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

const migrationUp = `
CREATE TABLE validations (
    id         INTEGER  PRIMARY KEY AUTOINCREMENT,
    message    TEXT     NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_validations_created_at ON validations (created_at);

CREATE TABLE ledger (
    id         INTEGER  PRIMARY KEY AUTOINCREMENT,
    message    TEXT     NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_ledger_created_at ON ledger (created_at);
`

func newTestEmbedded(t *testing.T) *Embedded {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec(migrationUp)
	require.NoError(t, err)

	t.Cleanup(func() { _ = db.Close() })
	return &Embedded{db: db}
}

func TestNewEmbedded_CreatesParentDir(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "nested", "deep")
	dbPath := filepath.Join(dir, "test.db")

	e, err := NewEmbedded(context.Background(), dbPath)
	require.NoError(t, err)
	defer func() { _ = e.Close(context.Background()) }()

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
}

func TestEmbedded_Close(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	e := &Embedded{db: db}
	require.NoError(t, e.Close(context.Background()))

	err = db.Ping()
	require.Error(t, err)
}
