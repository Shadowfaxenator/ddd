package domain

import (
	"context"
	"fmt"
	"strings"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/domain/command"
	"github.com/alekseev-bro/ddd/pkg/domain/event"
	"github.com/google/uuid"
)

type Aggregate[T aggregate.Aggregatable] interface {
	Create(ctx context.Context, body *T) (aggregate.ID[T], error)
	Update(ctx context.Context, id aggregate.ID[T], idempotencyKey string, handler func(body *T) (*T, error)) error
	StartService(ctx context.Context, handler Handler[T]) (aggregate.Drainer, error)
	aggregate.Aggregate[T]
}

type aggregateRoot[T aggregate.Aggregatable] struct {
	*aggregate.Root[T]
}

func (a *aggregateRoot[T]) Create(ctx context.Context, body *T) (aggregate.ID[T], error) {
	id := a.NewID()
	_, err := a.Execute(ctx, id.String(), &command.Create[T]{ID: id, Body: body})
	return id, err
}

func (a *aggregateRoot[T]) Update(ctx context.Context, id aggregate.ID[T], idempotencyKey string, handler func(body *T) (*T, error)) error {

	_, err := a.Execute(ctx, uuid.NewSHA1(id.UUID(), []byte(idempotencyKey)).String(), &command.Update[T]{ID: id, Handler: handler})
	return err
}

type Handler[T aggregate.Aggregatable] interface {
	Handle(context.Context, *T, aggregate.EventID[T]) error
}

type updateHandler[T aggregate.Aggregatable] struct {
	hfunc Handler[T]
}

func (h *updateHandler[T]) Handle(ctx context.Context, eventID aggregate.EventID[T], ev aggregate.Event[T]) error {
	switch e := ev.(type) {
	case *event.Updated[T]:
		return h.hfunc.Handle(ctx, e.Body, eventID)
	case *event.Created[T]:
		return h.hfunc.Handle(ctx, e.Body, eventID)
	default:
		return fmt.Errorf("unexpected event type %T", e)
	}
}

func (a *aggregateRoot[T]) StartService(ctx context.Context, handler Handler[T]) (aggregate.Drainer, error) {
	return a.Project(ctx, &updateHandler[T]{hfunc: handler}, aggregate.WithName(typereg.TypeNameFrom(handler)))
}

func NewAggregate[T aggregate.Aggregatable](ctx context.Context, es aggregate.EventStream[T], ss aggregate.SnapshotStore[T], opts ...aggregate.Option[T]) Aggregate[T] {
	aggr := aggregate.New(ctx, es, ss, opts...)
	typereg.Register(&event.Created[T]{})
	typereg.Register(&event.Updated[T]{})
	return &aggregateRoot[T]{Root: aggr}
}

func WithEvent[E aggregate.Event[T], T aggregate.Aggregatable]() aggregate.Option[T] {
	return func(a *aggregate.Root[T]) {
		var ev E

		if strings.Contains(typereg.TypeNameFrom(ev), event.InitialEventSuffix) {
			panic(fmt.Sprintf("events can't contain '%s' in their names, this is reserved for the aggregate initialization event", event.InitialEventSuffix))
		}
		typereg.Register(ev)
	}
}
