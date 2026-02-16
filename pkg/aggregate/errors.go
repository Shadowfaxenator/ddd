package aggregate

import "errors"

var (
	ErrNoAggregate            = errors.New("no aggregate messages")
	ErrNoStore                = errors.New("no store exists")
	ErrAggregateNotExists     = errors.New("aggregate is not created yet")
	ErrAggregateDeleted       = errors.New("aggregate is deleted")
	ErrAggregateAlreadyExists = errors.New("aggregate already exists")
)
