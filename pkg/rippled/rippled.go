package rippled

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// Client represents a rippled server client.
type Client struct {
	rootContext context.Context
	rootCancel  context.CancelFunc

	// The underlying websocket connection object.
	connection *websocket.Conn

	// All requests that have not been responded yet live here.
	pendingRequests SyncMap[string, chan<- subscriptionResponse]

	// Active or pending subscriptions.
	// Here:
	// 1. A key being absent means the subscription does not exist.
	// 2. A key's value being false means that the subscription is pending (request is processing)
	// 3. A key's value being true means that the subscription is active.
	subscriptions  SyncMap[string, bool]
	validationChan chan MessageValidationReceived

	// All errors that need to be transmitted to the Client-owner are sent to this channel.
	errorChan chan error

	// readLoopStopped receives an item only once readLoop returns.
	readLoopStopped chan struct{}
}

// NewClient returns a new Client instance.
// The addr is assumed to start with "wss://" or "ws://", no sanity checks are performed.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	rootContext, rootCancel := context.WithCancel(ctx)

	// Establish websocket connection with the rippled server.
	conn, response, err := websocket.Dial(rootContext, addr, nil)
	if err != nil {
		rootCancel()
		return nil, fmt.Errorf("error in websocket.Dial call: %w", err)
	}

	// Response body is not required, so close it right away.
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}

	client := &Client{
		rootContext:     rootContext,
		rootCancel:      rootCancel,
		connection:      conn,
		pendingRequests: SyncMap[string, chan<- subscriptionResponse]{},
		subscriptions:   SyncMap[string, bool]{},
		// This channel will receive validationReceived events.
		// The buffer should be large enough to accommodate a slow consumer.
		validationChan: make(chan MessageValidationReceived, 1000),
		// This channel will receive all errors.
		// The buffer should be large enough to accommodate a slow consumer.
		errorChan:       make(chan error, 1000),
		readLoopStopped: make(chan struct{}),
	}

	// Start reading messages.
	go client.readLoop(rootContext)

	return client, nil
}

// Close the client. If an error received from the Errors() method wraps ErrFatal, Close should be
// called by the owner manually.
func (c *Client) Close(ctx context.Context) error {
	// Canceling the root context will free the various operations that may otherwise block.
	c.rootCancel()

	if err := c.connection.Close(websocket.StatusNormalClosure, "internal reason"); err != nil {
		return fmt.Errorf("error in connection.Close call: %w", err)
	}

	// Channels like errorChan and validationChan are not closed here because the readLoop owns
	// them. Only the writer should close the channels.
	select {
	case <-c.readLoopStopped:
	// Respect the caller's context. Note that the maps won't be cleared in this case.
	case <-ctx.Done():
		return ctx.Err()
	}

	// Read loop has stopped. The final step is to clear the maps.
	c.pendingRequests.Clear()
	c.subscriptions.Clear()

	return nil
}

// Errors returns a read-only channel to which all internal errors of the Client are sent.
// The caller should consider the Client useless once an error is received.
func (c *Client) Errors() <-chan error {
	return c.errorChan
}

// SubscribeValidationStream subscribes to the "validations" stream.
func (c *Client) SubscribeValidationStream(ctx context.Context) (<-chan MessageValidationReceived, error) {
	// Mark the subscription in progress if there's no entry for it yet.
	if !c.subscriptions.StoreIfAbsent(messageTypeValidationReceived, false /* false means subscription is in progress */) {
		return nil, ErrStreamAlreadyOrBeingSubscribed
	}

	// If the subscription is still set to pending by the time the function is returning,
	// it means that subscription failed. So, it should be cleaned up.
	defer c.subscriptions.DeleteIf(messageTypeValidationReceived, func(v bool, exists bool) bool {
		return exists && v == false //nolint:staticcheck // v == false is more readable to me.
	})

	// ID to correlate request and response.
	id := uuid.NewString()

	// Form the request message.
	message, err := json.Marshal(subscriptionRequest{ID: id, Command: "subscribe", Streams: []string{"validations"}})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal subscription request: %w", err)
	}

	waitChan := make(chan subscriptionResponse)
	// Read loop will read this map and send the response to waitChan.
	c.pendingRequests.Store(id, waitChan)
	// Even though read loop owns this cleanup, doing it here again is harmless.
	defer c.pendingRequests.Delete(id)

	// Send the request message.
	if err := c.connection.Write(ctx, websocket.MessageText, message); err != nil {
		return nil, fmt.Errorf("failed to write subscription message to websocket: %w", err)
	}

	// Await response while respecting context.
	select {
	case response := <-waitChan:
		// Mostly a redundant check because read-loop matches IDs too, but doesn't hurt.
		if response.ID != id {
			return nil, fmt.Errorf("id mismatch, response id: %v, request id: %s", response.ID, id)
		}

		// Handle error response.
		if response.Status != responseStatusSuccess {
			return nil, fmt.Errorf("response status is not success: response: %+v", response)
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("context canceled before receiving response: %w", ctx.Err())
	case <-c.rootContext.Done():
		return nil, fmt.Errorf("root context canceled before receiving response: %w", c.rootContext.Err())
	}

	c.subscriptions.Store(messageTypeValidationReceived, true /* true means that the subscription is active. */)
	return c.validationChan, nil
}

func (c *Client) readLoop(ctx context.Context) {
	// Read loop is the only write to these channels so it is responsible for closing them.
	defer func() {
		close(c.validationChan)
		close(c.errorChan)
		close(c.readLoopStopped)
	}()

	for {
		// Read message. Error means that the connect is bad.
		messageType, message, err := c.connection.Read(ctx)
		if err != nil {
			err := fmt.Errorf("error while reading message: %w: %w", ErrFatal, err)
			sendContext(ctx, c.errorChan, err)
			return
		}

		// Ignore non-text messages.
		if messageType != websocket.MessageText {
			continue
		}

		// Get Ripple's message type ("response", "validationReceived" etc)
		rippleMessageType, err := getRippleMessageType(message)
		if err != nil {
			err := fmt.Errorf("failed to get ripple message type: %w", err)
			sendContext(ctx, c.errorChan, err)
			continue
		}

		switch rippleMessageType {
		case messageTypeResponse:
			var response subscriptionResponse
			if err := json.Unmarshal(message, &response); err != nil {
				err := fmt.Errorf("failed to unmarshal response message: %w", err)
				sendContext(ctx, c.errorChan, err)
				continue
			}

			// ID should be string for map lookup operation.
			id, ok := response.ID.(string)
			if !ok {
				err := fmt.Errorf("response id is not string: value: %v", response.ID)
				sendContext(ctx, c.errorChan, err)
				continue
			}

			// Find the corresponding request.
			channel, exists := c.pendingRequests.Load(id)
			if !exists {
				err := fmt.Errorf("no request found for response, id: %v", response.ID)
				sendContext(ctx, c.errorChan, err)
				continue
			}

			// Unblock request and cleanup.
			sendContext(ctx, channel, response)
			close(channel)
			c.pendingRequests.Delete(id)

		case messageTypeValidationReceived:
			//nolint:staticcheck
			// Explicit "status != true" check makes it clear that true represents an active subscription.
			if status, _ := c.subscriptions.Load(messageTypeValidationReceived); status != true {
				// Subscription is absent or pending, nothing to do.
				continue
			}

			var validationMessage MessageValidationReceived
			if err := json.Unmarshal(message, &validationMessage); err != nil {
				err := fmt.Errorf("failed to unmarshal %s message: %w", rippleMessageType, err)
				sendContext(ctx, c.errorChan, err)
				continue
			}

			sendContext(ctx, c.validationChan, validationMessage)

		default:
			err := fmt.Errorf("message of unknown type received: %s", rippleMessageType)
			sendContext(ctx, c.errorChan, err)
		}
	}
}
