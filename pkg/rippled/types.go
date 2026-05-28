package rippled

import (
	"errors"
)

const (
	responseStatusSuccess = "success"

	messageTypeResponse           = "response"
	messageTypeValidationReceived = "validationReceived"
)

var (
	ErrStreamAlreadyOrBeingSubscribed = errors.New("stream is already subscribed or being subscribed")

	// ErrFatal means that the Client is no longer usable.
	ErrFatal = errors.New("fatal error")
)

// MessageValidationReceived is the schema of a message received through the "validation" stream.
type MessageValidationReceived struct{}

// subscriptionRequest is the schema of a subscription request for rippled.
type subscriptionRequest struct {
	ID      any      `json:"id"`
	Command string   `json:"command"`
	Streams []string `json:"streams"`
}

// subscriptionResponse is the schema of the response that rippled gives for a subscription request.
type subscriptionResponse struct {
	ID     any    `json:"id"`
	Status string `json:"status"`
	Type   string `json:"type"`
	Result any    `json:"result"`
}
