package aggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/id"
)

type ID id.ID
type EventID id.ID

func (i ID) I64() int64 {
	return int64(i)
}

func NewEventID() (EventID, error) {
	i, err := id.New()
	if err != nil {
		return EventID(0), err
	}
	return EventID(i), nil
}

func NewID() (ID, error) {
	i, err := id.New()
	if err != nil {
		return ID(0), err
	}
	return ID(i), nil
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
	ID        ID
	Sequence  uint64
	Timestamp time.Time
	Version   uint64
	State     *T
}
