package rippled

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestNewClient_Success(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	require.NotNil(t, client)

	_ = client.Close(context.Background())
}

func TestNewClient_DialFailure(t *testing.T) {
	t.Parallel()

	client, err := NewClient(context.Background(), "ws://127.0.0.1:1")
	require.Error(t, err)
	require.Nil(t, client)
}

func TestClient_Close(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)

	err = client.Close(context.Background())
	require.NoError(t, err)

	// Verify channels are eventually closed (drain any pending values first).
	requireChanClosed(t, client.Errors())
	requireChanClosed(t, client.validationChan)
}

func TestClient_CloseRespectsContext(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		// Keep connection alive — don't exit until the test server shuts down.
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.Close(canceledCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestClient_SubscribeValidationStream_Success(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		req, err := respondToSubscribe(ctx, conn)
		if err != nil {
			return
		}

		require.Equal(t, "subscribe", req.Command)
		require.Equal(t, []string{"validations"}, req.Streams)

		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	ch, err := client.SubscribeValidationStream(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ch)
}

func TestClient_SubscribeValidationStream_ErrorResponse(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return
		}

		var req subscriptionRequest
		_ = json.Unmarshal(msg, &req)

		resp, _ := json.Marshal(subscriptionResponse{
			ID:     req.ID,
			Type:   messageTypeResponse,
			Status: "error",
		})

		_ = conn.Write(ctx, websocket.MessageText, resp)
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	ch, err := client.SubscribeValidationStream(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not success")
	require.Nil(t, ch)
}

func TestClient_SubscribeValidationStream_ContextCanceled(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		// Read but never respond.
		_, _, _ = conn.Read(ctx)
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ch, err := client.SubscribeValidationStream(ctx)
	require.Error(t, err)
	require.Nil(t, ch)
}

func TestClient_SubscribeValidationStream_DuplicateSubscription(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		_, _ = respondToSubscribe(ctx, conn)
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	_, err = client.SubscribeValidationStream(context.Background())
	require.NoError(t, err)

	_, err = client.SubscribeValidationStream(context.Background())
	require.ErrorIs(t, err, ErrStreamAlreadyOrBeingSubscribed)
}

func TestClient_ReadLoop_ValidationMessages(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := respondToSubscribe(ctx, conn); err != nil {
			return
		}

		hashes := []string{"AAA", "BBB", "CCC"}
		for _, h := range hashes {
			msg, _ := json.Marshal(map[string]any{
				"type":        messageTypeValidationReceived,
				"ledger_hash": h,
			})
			if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
				return
			}
		}

		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	ch, err := client.SubscribeValidationStream(context.Background())
	require.NoError(t, err)

	expected := []string{"AAA", "BBB", "CCC"}
	for i, want := range expected {
		select {
		case msg := <-ch:
			require.Equal(t, want, msg.LedgerHash)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestClient_ReadLoop_UnknownMessageType(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		msg, _ := json.Marshal(map[string]any{
			"type": "somethingWeird",
		})
		_ = conn.Write(ctx, websocket.MessageText, msg)
		<-ctx.Done()
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	select {
	case err := <-client.Errors():
		require.Contains(t, err.Error(), "unknown type")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error")
	}
}

func TestClient_ReadLoop_ServerDisconnect(t *testing.T) {
	t.Parallel()

	url := startMockServer(t, func(ctx context.Context, conn *websocket.Conn) {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	})

	client, err := NewClient(context.Background(), url)
	require.NoError(t, err)

	// Should receive a fatal error.
	select {
	case err := <-client.Errors():
		require.True(t, errors.Is(err, ErrFatal))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fatal error")
	}

	// Channels should be closed after readLoop exits.
	select {
	case _, open := <-client.validationChan:
		require.False(t, open)
	case <-time.After(2 * time.Second):
		t.Fatal("validationChan not closed")
	}
}
