package domain

import (
	"context"
	"fmt"
	"strings"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
)

const (
	initialEventSuffix   = "Created"
	initialCommandPrefix = "Create"
	lastEventSuffix      = "Deleted"
	lastCommandPrefix    = "Delete"
)

func NewAggregate[T aggregate.Aggregatable](ctx context.Context, es aggregate.EventStream[T], ss aggregate.SnapshotStore[T], opts ...aggregate.Option[T]) *aggregate.Root[T] {
	aggr := aggregate.New(ctx, es, ss, opts...)
	typereg.Register(&Created[T]{})
	return aggr
}

func WithEvent[E aggregate.Event[T], T aggregate.Aggregatable]() aggregate.Option[T] {
	return func(a *aggregate.Root[T]) {
		var ev E

		if strings.Contains(typereg.TypeNameFrom(ev), initialEventSuffix) {
			panic(fmt.Sprintf("events can't contain '%s' in their names, this is reserved for the aggregate initialization event", initialEventSuffix))
		}
		typereg.Register(ev)
	}
}
