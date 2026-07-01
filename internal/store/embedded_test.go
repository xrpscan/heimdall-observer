package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"

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

	messages := []xrpld.MessageValidationReceived{
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

	messages := []xrpld.MessageValidationReceived{
		{LedgerHash: "DEADBEEF", ValidationPublicKey: "nHB1X37qrni", Full: true, Flags: 0x80000001},
	}

	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	var stored string
	err := e.db.QueryRowContext(ctx, "SELECT message FROM validations LIMIT 1").Scan(&stored)
	require.NoError(t, err)

	var parsed xrpld.MessageValidationReceived
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

	batch1 := []xrpld.MessageValidationReceived{{LedgerHash: "A"}, {LedgerHash: "B"}}
	batch2 := []xrpld.MessageValidationReceived{{LedgerHash: "C"}}

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

	messages := []xrpld.MessageValidationReceived{{LedgerHash: "A"}, {LedgerHash: "B"}}
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

	messages := []xrpld.MessageValidationReceived{{LedgerHash: "X"}}
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

func TestList_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageValidationReceived{
		{LedgerHash: "AAA"},
		{LedgerHash: "BBB"},
		{LedgerHash: "CCC"},
	}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	rows, err := e.ListValidationMessages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	require.Equal(t, 1, rows[0].ID)
	require.Equal(t, "AAA", rows[0].Message.LedgerHash)
	require.NotZero(t, rows[0].CreatedAt)

	require.Equal(t, 2, rows[1].ID)
	require.Equal(t, "BBB", rows[1].Message.LedgerHash)

	require.Equal(t, 3, rows[2].ID)
	require.Equal(t, "CCC", rows[2].Message.LedgerHash)
}

func TestList_RespectsLimit(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageValidationReceived{
		{LedgerHash: "A"}, {LedgerHash: "B"}, {LedgerHash: "C"},
		{LedgerHash: "D"}, {LedgerHash: "E"},
	}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	rows, err := e.ListValidationMessages(ctx, 2)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestList_Empty(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	rows, err := e.ListValidationMessages(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestList_MessageContentRoundTrip(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	original := xrpld.MessageValidationReceived{
		LedgerHash:          "DEADBEEF",
		ValidationPublicKey: "nHB1X37qrni",
		Full:                true,
		Flags:               0x80000001,
		SigningTime:         1234567890,
	}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, []xrpld.MessageValidationReceived{original}))

	rows, err := e.ListValidationMessages(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	got := rows[0].Message
	require.Equal(t, original.LedgerHash, got.LedgerHash)
	require.Equal(t, original.ValidationPublicKey, got.ValidationPublicKey)
	require.Equal(t, original.Full, got.Full)
	require.Equal(t, original.Flags, got.Flags)
	require.Equal(t, original.SigningTime, got.SigningTime)
}

func TestDelete_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageValidationReceived{
		{LedgerHash: "A"}, {LedgerHash: "B"}, {LedgerHash: "C"},
	}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	require.NoError(t, e.DeleteValidationMessages(ctx, []int{1, 2}))

	rows, err := e.ListValidationMessages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "C", rows[0].Message.LedgerHash)
}

func TestDelete_NonExistentID(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	require.NoError(t, e.BulkInsertValidationMessages(ctx, []xrpld.MessageValidationReceived{{LedgerHash: "A"}}))

	err := e.DeleteValidationMessages(ctx, []int{999})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected number of rows were deleted")
}

func TestDelete_PartialMatch(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageValidationReceived{{LedgerHash: "A"}, {LedgerHash: "B"}}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	err := e.DeleteValidationMessages(ctx, []int{1, 999})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected number of rows were deleted")
}

func TestInsertListDelete_RoundTrip(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageValidationReceived{
		{LedgerHash: "X"}, {LedgerHash: "Y"}, {LedgerHash: "Z"},
	}
	require.NoError(t, e.BulkInsertValidationMessages(ctx, messages))

	rows, err := e.ListValidationMessages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	ids := make([]int, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	require.NoError(t, e.DeleteValidationMessages(ctx, ids))

	rows, err = e.ListValidationMessages(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, rows)
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
