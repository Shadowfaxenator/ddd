package command

import (
	"fmt"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/domain/event"
)

const (
	InitialCommandPrefix = "Create"

	UpdateCommandPrefix = "Update"
)

var ErrAggregateNotInitialized = fmt.Errorf("aggregate not initialized")

type Create[T aggregate.Aggregatable] struct {
	ID   aggregate.ID[T]
	Body *T
}

func (c *Create[T]) AggregateID() aggregate.ID[T] {
	return c.ID
}
func (c *Create[T]) Execute(entity *T) (aggregate.Event[T], error) {
	if entity == nil {

		return &event.Created[T]{ID: c.ID, Body: c.Body}, nil
	}
	return nil, ErrAggregateNotInitialized
}

func (c *Create[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", InitialCommandPrefix, typereg.TypeNameFrom(zero))
}

type Update[T aggregate.Aggregatable] struct {
	ID      aggregate.ID[T]
	Handler func(body *T) (*T, error)
}

func (c *Update[T]) AggregateID() aggregate.ID[T] {
	return c.ID
}
func (c *Update[T]) Execute(entity *T) (aggregate.Event[T], error) {
	if entity == nil {
		return nil, ErrAggregateNotInitialized
	}

	result, err := c.Handler(entity)
	if err != nil {
		return nil, err
	}

	return &event.Updated[T]{Body: result}, nil

}

func (c *Update[T]) String() string {
	var zero T
	return fmt.Sprintf("%s%s", UpdateCommandPrefix, typereg.TypeNameFrom(zero))
}

type Delete[T aggregate.Aggregatable] struct {
	ID aggregate.ID[T]
}
