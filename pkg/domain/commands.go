package domain

import (
	"fmt"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
)

type Create[T aggregate.Aggregatable] struct {
	ID   aggregate.ID[T]
	Body *T
}

func (c *Create[T]) AggregateID() aggregate.ID[T] {
	return c.ID
}
func (c *Create[T]) Execute(entity *T) aggregate.Event[T] {
	if entity == nil {

		return &Created[T]{ID: c.ID, Body: c.Body}
	}
	return &aggregate.EventError[T]{AggID: c.ID, Reason: "aggregate already initialized"}
}

func (c *Create[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", initialCommandPrefix, typereg.TypeNameFrom(zero))
}

type Delete[T aggregate.Aggregatable] struct {
	ID aggregate.ID[T]
}

func (c *Delete[T]) AggregateID() aggregate.ID[T] {
	return c.ID
}
func (c *Delete[T]) Execute(entity *T) aggregate.Event[T] {
	if entity == nil {

		return &Deleted[T]{ID: c.ID}
	}
	return &aggregate.EventError[T]{AggID: c.ID, Reason: "aggregate already initialized"}
}

func (c *Delete[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", lastCommandPrefix, typereg.TypeNameFrom(zero))
}
