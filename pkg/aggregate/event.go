package aggregate

type EventID[T Aggregatable] = ID[Event[T]]

// You can use EventError to create an error event that should be not stored to the event store.
type EventError[T Aggregatable] struct {
	AggID  ID[T]
	Reason string
}

func (e EventError[T]) Error() string {
	return e.Reason
}

func (e *EventError[T]) Apply(*T) {}

type Event[T Aggregatable] interface {
	Apply(*T)
}
