package eventstore

type EventID[T any] = ID[Applyer[T]]

type Applyer[T any] interface {
	Apply(*T)
}

func NewEvents[T any](events ...Applyer[T]) Events[T] {
	return events
}

type Events[T any] []Applyer[T]

type Event[T any] struct {
	ID          EventID[T]
	PrevVersion uint64
	Version     uint64
	Kind        string
	Body        Applyer[T]
}
