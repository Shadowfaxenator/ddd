package event

import (
	"fmt"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
)

const (
	InitialEventSuffix = "Created"

	UpdatedEventSuffix = "Updated"
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
	return fmt.Sprintf("%s%s", typereg.TypeNameFrom(zero), InitialEventSuffix)
}

type Updated[T aggregate.Aggregatable] struct {
	Body *T
}

func (e *Updated[T]) Apply(a *T) {

	*a = *e.Body
}

func (e *Updated[T]) String() string {
	var zero T

	return fmt.Sprintf("%s%s", typereg.TypeNameFrom(zero), UpdatedEventSuffix)
}
