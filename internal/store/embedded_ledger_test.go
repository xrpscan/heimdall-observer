package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xrpscan/heimdall-observer/pkg/xrpld"

	"github.com/stretchr/testify/require"
)

func TestBulkInsertLedger_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "AAA", TxnCount: 10},
		{LedgerHash: "BBB", TxnCount: 20},
		{LedgerHash: "CCC", TxnCount: 30},
	}

	err := e.BulkInsertLedgerMessages(ctx, messages)
	require.NoError(t, err)

	var count int
	err = e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestBulkInsertLedger_MessageContent(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "DEADBEEF", TxnCount: 42, ReserveBase: 10000000, ReserveInc: 2000000},
	}

	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	var stored string
	err := e.db.QueryRowContext(ctx, "SELECT message FROM ledger LIMIT 1").Scan(&stored)
	require.NoError(t, err)

	var parsed xrpld.MessageLedgerClosed
	require.NoError(t, json.Unmarshal([]byte(stored), &parsed))
	require.Equal(t, "DEADBEEF", parsed.LedgerHash)
	require.Equal(t, 42, parsed.TxnCount)
	require.Equal(t, uint(10000000), parsed.ReserveBase)
	require.Equal(t, uint(2000000), parsed.ReserveInc)
}

func TestBulkInsertLedger_MultipleBatches(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	batch1 := []xrpld.MessageLedgerClosed{{LedgerHash: "A"}, {LedgerHash: "B"}}
	batch2 := []xrpld.MessageLedgerClosed{{LedgerHash: "C"}}

	require.NoError(t, e.BulkInsertLedgerMessages(ctx, batch1))
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, batch2))

	var count int
	err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestBulkInsertLedger_CreatedAtPopulated(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{{LedgerHash: "X"}}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	var createdAt string
	err := e.db.QueryRowContext(ctx, "SELECT created_at FROM ledger LIMIT 1").Scan(&createdAt)
	require.NoError(t, err)
	require.NotEmpty(t, createdAt)
}

func TestBulkInsertLedger_DoesNotAffectValidations(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	require.NoError(t, e.BulkInsertLedgerMessages(ctx, []xrpld.MessageLedgerClosed{{LedgerHash: "L1"}}))

	var count int
	err := e.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM validations").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestListLedger_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "AAA", TxnCount: 10},
		{LedgerHash: "BBB", TxnCount: 20},
		{LedgerHash: "CCC", TxnCount: 30},
	}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	rows, err := e.ListLedgerMessages(ctx, 10)
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

func TestListLedger_RespectsLimit(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "A"}, {LedgerHash: "B"}, {LedgerHash: "C"},
		{LedgerHash: "D"}, {LedgerHash: "E"},
	}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	rows, err := e.ListLedgerMessages(ctx, 2)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestListLedger_Empty(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	rows, err := e.ListLedgerMessages(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestListLedger_MessageContentRoundTrip(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	original := xrpld.MessageLedgerClosed{
		LedgerHash:       "DEADBEEF",
		TxnCount:         42,
		ReserveBase:      10000000,
		ReserveInc:       2000000,
		ValidatedLedgers: "100-200",
	}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, []xrpld.MessageLedgerClosed{original}))

	rows, err := e.ListLedgerMessages(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	got := rows[0].Message
	require.Equal(t, original.LedgerHash, got.LedgerHash)
	require.Equal(t, original.TxnCount, got.TxnCount)
	require.Equal(t, original.ReserveBase, got.ReserveBase)
	require.Equal(t, original.ReserveInc, got.ReserveInc)
	require.Equal(t, original.ValidatedLedgers, got.ValidatedLedgers)
}

func TestDeleteLedger_Success(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "A"}, {LedgerHash: "B"}, {LedgerHash: "C"},
	}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	require.NoError(t, e.DeleteLedgerMessages(ctx, []LedgerMessageRow{{ID: 1}, {ID: 2}}))

	rows, err := e.ListLedgerMessages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "C", rows[0].Message.LedgerHash)
}

func TestDeleteLedger_NonExistentID(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	require.NoError(t, e.BulkInsertLedgerMessages(ctx, []xrpld.MessageLedgerClosed{{LedgerHash: "A"}}))

	err := e.DeleteLedgerMessages(ctx, []LedgerMessageRow{{ID: 999}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected number of rows were deleted")
}

func TestDeleteLedger_PartialMatch(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{{LedgerHash: "A"}, {LedgerHash: "B"}}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	err := e.DeleteLedgerMessages(ctx, []LedgerMessageRow{{ID: 1}, {ID: 999}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected number of rows were deleted")
}

func TestInsertListDeleteLedger_RoundTrip(t *testing.T) {
	t.Parallel()

	e := newTestEmbedded(t)
	ctx := context.Background()

	messages := []xrpld.MessageLedgerClosed{
		{LedgerHash: "X"}, {LedgerHash: "Y"}, {LedgerHash: "Z"},
	}
	require.NoError(t, e.BulkInsertLedgerMessages(ctx, messages))

	rows, err := e.ListLedgerMessages(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	require.NoError(t, e.DeleteLedgerMessages(ctx, rows))

	rows, err = e.ListLedgerMessages(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, rows)
}
