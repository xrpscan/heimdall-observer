package rippled

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// Client represents a rippled server client.
type Client struct {
	connection *websocket.Conn

	// All requests that have not been responded yet live here.
	pendingRequests SyncMap[string, chan<- subscriptionResponse]

	// Active subscriptions.
	subscriptions  SyncMap[string, struct{}]
	validationChan chan MessageValidationReceived

	// All errors that need to be transmitted to the Client-owner are sent to this channel.
	errorChan chan error
}

// NewClient returns a new Client instance.
// The addr is assumed to start with "wss://" or "ws://", no sanity checks are performed.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	// Establish websocket connection with the rippled server.
	conn, response, err := websocket.Dial(ctx, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("error in websocket.Dial call: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	// This channel will receive all errors.
	errorChan := make(chan error, 10)
	// This channel will receive validationReceived events.
	validationChan := make(chan MessageValidationReceived, 10)

	client := &Client{
		connection:      conn,
		pendingRequests: SyncMap[string, chan<- subscriptionResponse]{},
		subscriptions:   SyncMap[string, struct{}]{},
		validationChan:  validationChan,
		errorChan:       errorChan,
	}

	// Start reading messages.
	go client.readLoop(ctx)

	return client, nil
}

// Close the client. If an error received from the Errors() method wraps ErrFatal, Close should be
// called by the owner manually.
func (c *Client) Close(reason string) error {
	if err := c.connection.Close(websocket.StatusNormalClosure, reason); err != nil {
		return fmt.Errorf("error in connection.Close call: %w", err)
	}

	close(c.errorChan)
	close(c.validationChan)

	c.pendingRequests.Clear()
	c.subscriptions.Clear()
	return nil
}

// Errors returns a read-only channel to which all interal errors of the Client are sent.
// The caller should consider the Client useless once an error is received.
func (c *Client) Errors() <-chan error {
	return c.errorChan
}

// SubscribeValidationStream subscribes to the "validations" stream.
func (c *Client) SubscribeValidationStream(ctx context.Context) (<-chan MessageValidationReceived, error) {
	// Disallow multiple subscriptions.
	if _, exists := c.subscriptions.Load(messageTypeValidationReceived); exists {
		return nil, ErrStreamAlreadySubscribed
	}

	// ID to correlate request and response.
	id := uuid.NewString()

	// Form the request message.
	message, err := json.Marshal(subscriptionRequest{ID: id, Command: "subscribe", Stream: []string{"validations"}})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal subscription request: %w", err)
	}

	waitChan := make(chan subscriptionResponse)
	// Read loop will read this map and send the response to waitChan.
	c.pendingRequests.Store(id, waitChan)
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
			return nil, fmt.Errorf("id mismatch, response id: %s, request id: %s", response.ID, id)
		}

		// Handle error response.
		if response.Status != responseStatusSuccess {
			return nil, fmt.Errorf("response status is not success: response: %+v", response)
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("context canceled before receving response: %w", ctx.Err())
	}

	c.subscriptions.Store(messageTypeValidationReceived, struct{}{})
	return c.validationChan, nil
}

func (c *Client) readLoop(ctx context.Context) {
	for {
		// Read message. Error means that the connect is bad.
		messageType, message, err := c.connection.Read(ctx)
		if err != nil {
			c.errorChan <- errors.Join(fmt.Errorf("error while reading message: %w", err), ErrFatal)
			return
		}

		// Ignore non-text messages.
		if messageType != websocket.MessageText {
			continue
		}

		// Get Ripple's message type ("response", "validationReceived" etc)
		rippleMessageType, err := getRippleMessageType(message)
		if err != nil {
			c.errorChan <- fmt.Errorf("failed to get ripple message type: %w", err)
			continue
		}

		switch rippleMessageType {
		case messageTypeResponse:
			var response subscriptionResponse
			if err := json.Unmarshal(message, &response); err != nil {
				c.errorChan <- fmt.Errorf("failed to unmarshal response message: %w", err)
				continue
			}

			// ID should be string for map lookup operation.
			id, ok := response.ID.(string)
			if !ok {
				c.errorChan <- fmt.Errorf("response id is not string: value: %v", response.ID)
				continue
			}

			// Find the corresponding request.
			channel, exists := c.pendingRequests.Load(id)
			if !exists {
				c.errorChan <- fmt.Errorf("no request found for response, id: %s", response.ID)
				continue
			}

			// Unblock request and cleanup.
			channel <- response
			close(channel)
			c.pendingRequests.Delete(id)

		case messageTypeValidationReceived:
			if _, subscribed := c.subscriptions.Load(messageTypeValidationReceived); !subscribed {
				// Not subscribed, nothing to do.
				continue
			}

			var validationMessage MessageValidationReceived
			if err := json.Unmarshal(message, &validationMessage); err != nil {
				c.errorChan <- fmt.Errorf("failed to unmarshal %s message: %w", rippleMessageType, err)
				continue
			}

			c.validationChan <- validationMessage
		}
	}
}
