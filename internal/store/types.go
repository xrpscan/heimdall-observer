package store

import (
	"github.com/shivanshkc/observer/pkg/rippled"
)

// ValidationMessageRow is the schema of a single row in the "validations" table.
type ValidationMessageRow struct {
	Id        int
	Message   rippled.MessageValidationReceived
	CreatedAt int64
}
