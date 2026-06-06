package store

import (
	"github.com/shivanshkc/observer/pkg/rippled"
)

// ValidationReceivedMessageRow is the schema of a single row in the "validations" table.
type ValidationReceivedMessageRow struct {
	Id        int
	Message   rippled.MessageValidationReceived
	CreatedAt int64
}
