package rippled

import (
	"context"
	"encoding/json"
	"fmt"
)

// getRippleMessageType tries to parse the give json message and get the "type" key from it.
func getRippleMessageType(message []byte) (string, error) {
	var typeDecoder struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(message, &typeDecoder); err != nil {
		return "", fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if typeDecoder.Type == "" {
		return "", fmt.Errorf("type field is empty or absent")
	}

	return typeDecoder.Type, nil
}

// Sends the given item to the given channel with context-awareness.
func sendContext[T any](ctx context.Context, c chan<- T, item T) {
	select {
	case <-ctx.Done():
	case c <- item:
	}
}
