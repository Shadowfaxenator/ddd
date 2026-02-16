package natsstore

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/drivers/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/pkg/drivers/stream/esnats"
	"github.com/alekseev-bro/ddd/pkg/eventstore"

	"github.com/nats-io/nats.go/jetstream"
)

type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type options[T any, PT eventstore.StatePtr[T]] struct {
	esCfg  []esnats.Option
	ssCfg  []snapnats.Option
	agOpts []eventstore.StoreOption[T, PT]
}

type option[T any, PT eventstore.StatePtr[T]] func(c *options[T, PT])

func WithInMemory[T any, PT eventstore.StatePtr[T]]() option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg = append(opts.esCfg, esnats.WithStoreType(esnats.Memory))
		opts.ssCfg = append(opts.ssCfg, snapnats.WithStoreType(snapnats.Memory))
	}
}

func WithDeduplication[T any, PT eventstore.StatePtr[T]](duration time.Duration) option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg = append(opts.esCfg, esnats.WithDeduplication(duration))
	}
}

func WithEvent[E any, T any, PE interface {
	*E
	aggregate.Evolver[T]
}, PT eventstore.StatePtr[T]](name string) option[T, PT] {
	return func(o *options[T, PT]) {
		o.agOpts = append(o.agOpts, eventstore.WithEvent[E, T, PE, PT](name))
	}
}

func WithSnapshot[T any, PT eventstore.StatePtr[T]](maxMsgs byte, maxInterval time.Duration, timeout time.Duration) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, eventstore.WithSnapshot[T, PT](maxMsgs, maxInterval, timeout))
	}
}

func WithSnapshotCodec[T any, PT eventstore.StatePtr[T]](codec codec.Codec) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, eventstore.WithCodec[T, PT](codec))
	}
}

func WithLogger[T any, PT eventstore.StatePtr[T]](logger Logger) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, eventstore.WithLogger[T, PT](logger))
		a.esCfg = append(a.esCfg, esnats.WithLogger(logger))
	}
}
