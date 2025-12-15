package domain

import (
	"context"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"

	"github.com/nats-io/nats.go/jetstream"
)

type registry interface {
	register(any)
}

func RegisterEvent[E aggregate.Event[T], T any]() {
	var ev E
	typereg.Register(ev)
}

func NewNatsAggregate[T any](ctx context.Context, js jetstream.JetStream, opts ...option[T]) aggregate.Aggregate[T] {
	op := options[T]{}
	for _, opt := range opts {
		opt(&op)
	}
	es := esnats.NewEventStream[T](ctx, js, op.esOpts...)
	ss := snapnats.NewSnapshotStore[T](ctx, js, op.ssOpts...)
	return aggregate.New[T](ctx, es, ss, op.agOpts...)
}
