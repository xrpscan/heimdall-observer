package rippled

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// startMockServer spins up an httptest server that upgrades to WebSocket and runs the given handler.
// Returns the ws:// URL. The server is closed automatically when the test ends.
func startMockServer(t *testing.T, handler func(ctx context.Context, conn *websocket.Conn)) string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		handler(r.Context(), conn)
	}))

	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

// respondToSubscribe is a helper that reads a subscribe request and responds with success.
// Returns the parsed request for assertion if needed.
func respondToSubscribe(ctx context.Context, conn *websocket.Conn) (subscriptionRequest, error) {
	_, msg, err := conn.Read(ctx)
	if err != nil {
		return subscriptionRequest{}, err
	}

	var req subscriptionRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		return subscriptionRequest{}, err
	}

	resp, _ := json.Marshal(subscriptionResponse{
		ID:     req.ID,
		Type:   messageTypeResponse,
		Status: responseStatusSuccess,
	})

	if err := conn.Write(ctx, websocket.MessageText, resp); err != nil {
		return subscriptionRequest{}, err
	}

	return req, nil
}

// requireChanClosed drains a channel and asserts it closes within 2 seconds.
func requireChanClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("channel not closed within timeout")
		}
	}
}
