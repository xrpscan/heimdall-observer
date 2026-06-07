package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shivanshkc/observer/pkg/rippled"

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

func TestBulkInsert_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []rippled.MessageValidationReceived{
		{LedgerHash: "AAA", Full: true},
		{LedgerHash: "BBB", Full: false},
		{LedgerHash: "CCC", Full: true},
	}

	err := e.BulkInsertValidationMessages(ctx, messages)
	require.NoError(t, err)

	var count int
	err = e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM validations").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestBulkInsert_MessageContent(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []rippled.MessageValidationReceived{
		{LedgerHash: "DEADBEEF", ValidationPublicKey: "nHB1X37qrni", Full: true, Flags: 0x80000001},
	}

	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	var stored string
	err := e.db.QueryRowContext(ctx, "SELECT message FROM validations LIMIT 1").Scan(&stored)
	require.NoError(t, err)

	var parsed rippled.MessageValidationReceived
	require.NoError(t, json.Unmarshal([]byte(stored), &parsed))
	require.Equal(t, "DEADBEEF", parsed.LedgerHash)
	require.Equal(t, "nHB1X37qrni", parsed.ValidationPublicKey)
	require.True(t, parsed.Full)
	require.Equal(t, uint32(0x80000001), parsed.Flags)
}

func TestBulkInsert_MultipleBatches(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	batch1 := []rippled.MessageValidationReceived{{LedgerHash: "A"}, {LedgerHash: "B"}}
	batch2 := []rippled.MessageValidationReceived{{LedgerHash: "C"}}

	require.NoError(t, e.BulkInsertValidationMessages(ctx, batch1))
	require.NoError(t, e.BulkInsertValidationMessages(ctx, batch2))

	var count int
	err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM validations").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestBulkInsert_AutoIncrementIDs(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []rippled.MessageValidationReceived{{LedgerHash: "A"}, {LedgerHash: "B"}}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	rows, err := e.db.QueryContext(ctx, "SELECT id FROM validations ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{1, 2}, ids)
}

func TestBulkInsert_CreatedAtPopulated(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []rippled.MessageValidationReceived{{LedgerHash: "X"}}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	var createdAt string
	err := e.db.QueryRowContext(ctx, "SELECT created_at FROM validations LIMIT 1").Scan(&createdAt)
	require.NoError(t, err)
	require.NotEmpty(t, createdAt)
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
