package aggregate

type Evolver[T any] interface {
	Evolve(*T)
}

func NewEvents[T any](events ...Evolver[T]) Events[T] {
	return events
}

type Events[T any] []Evolver[T]

func (e Events[T]) Evolve(aggr *T) {
	for _, ev := range e {
		ev.Evolve(aggr)
	}
}
