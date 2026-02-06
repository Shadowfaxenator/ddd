package aggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/id"
)

type ID id.ID
type EventID id.ID

func NewEventID() EventID {
	return EventID(id.New())
}

func NewID() ID {
	return ID(id.New())
}

func (i EventID) String() string {
	return id.ID(i).String()
}
func (i ID) String() string {
	return id.ID(i).String()
}

type InvariantViolationError struct {
	Err error
}

func (e InvariantViolationError) Error() string {
	return e.Err.Error()
}

type Aggregate[T any] struct {
	ID
	Sequence  uint64
	Timestamp time.Time
	State     T
}
