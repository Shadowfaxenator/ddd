package natsstore

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/driver/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/internal/driver/stream/esnats"
	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/eventstore"

	"github.com/nats-io/nats.go/jetstream"
)

func New[T any, PT eventstore.PRoot[T]](ctx context.Context, js jetstream.JetStream, opts ...option[T, PT]) eventstore.Store[T, PT] {
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
