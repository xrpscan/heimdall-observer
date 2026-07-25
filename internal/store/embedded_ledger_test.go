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
