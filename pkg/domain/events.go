package domain

import (
	"fmt"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
)

type Created[T aggregate.Aggregatable] struct {
	ID   aggregate.ID[T]
	Body *T
	// Kind string
}

func (e *Created[T]) Apply(a *T) {
	*a = *e.Body
}

func (e *Created[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", typereg.TypeNameFrom(zero), initialEventSuffix)
}

type Deleted[T any] struct {
	ID aggregate.ID[T]
}

func (e *Deleted[T]) Apply(a *T) {

}

func (e *Deleted[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", typereg.TypeNameFrom(zero), lastEventSuffix)
}
