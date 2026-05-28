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
	ErrStreamAlreadySubscribed = errors.New("stream is already subscribed")

	// ErrFatal means that the Client is no longer usable.
	ErrFatal = errors.New("fatal error")
)

// MessageValidationReceived is the schema of a message received through the "validation" stream.
type MessageValidationReceived struct{}

type subscriptionRequest struct {
	ID      any      `json:"id"`
	Command string   `json:"command"`
	Stream  []string `json:"stream"`
}

type subscriptionResponse struct {
	ID     any    `json:"id"`
	Status string `json:"status"`
	Type   string `json:"type"`
	Result any    `json:"result"`
}
