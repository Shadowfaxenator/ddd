package natsstore

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/drivers/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/pkg/drivers/stream/esnats"
	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/alekseev-bro/ddd/pkg/stream"

	"github.com/nats-io/nats.go/jetstream"
)

type Store[T any, PT eventstore.PRoot[T]] struct {
	js jetstream.JetStream
}

func New[T any, PT eventstore.PRoot[T]](ctx context.Context, js jetstream.JetStream, opts ...option[T, PT]) *eventstore.Store[T, PT] {
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		panic("type T is not a struct")
	}
	cfg := &options[T, PT]{}
	for _, opt := range opts {
		opt(cfg)
	}
	strName := fmt.Sprintf("%s", typereg.TypeNameFor[T](typereg.WithDelimiter(":")))
	es := esnats.NewDriver(ctx, js, strName, cfg.esCfg)
	ss := snapnats.NewDriver(ctx, js, typereg.TypeNameFor[T](typereg.WithDelimiter("-")), cfg.ssCfg)

	return eventstore.New(ctx, es, ss, cfg.agOpts...)
}

type saver interface {
	Save(ctx context.Context, aggrID aggregate.ID, expectedSequence uint64, events []any) ([]stream.MsgMetadata, error)
}

type subscriber interface {
	Subscribe(ctx context.Context, h stream.EventHandler, opts ...stream.ProjOption) (stream.Drainer, error)
}

type Stream interface {
	subscriber
	saver
}
