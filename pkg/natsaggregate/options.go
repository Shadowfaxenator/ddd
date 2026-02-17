package natsaggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/drivers/snapshot/natssnapshot"
	"github.com/alekseev-bro/ddd/pkg/drivers/stream/natsstream"
	"github.com/alekseev-bro/ddd/pkg/snapshot"

	"github.com/nats-io/nats.go/jetstream"
)

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type options[T any] struct {
	streamName        string
	snapshotStoreName string
	esCfg             []natsstream.Option
	ssCfg             []natssnapshot.Option
	agOpts            []aggregate.Option[T]
}

type option[T any] func(c *options[T])

func WithStreamName[T any](name string) option[T] {
	return func(opts *options[T]) {
		opts.streamName = name
	}
}

func WithSnapshotStoreName[T any](name string) option[T] {
	return func(opts *options[T]) {
		opts.snapshotStoreName = name
	}
}

func WithInMemory[T any]() option[T] {
	return func(opts *options[T]) {
		opts.esCfg = append(opts.esCfg, natsstream.WithStoreType(natsstream.Memory))
		opts.ssCfg = append(opts.ssCfg, natssnapshot.WithStoreType(natssnapshot.Memory))
	}
}

func WithSnapshotMaxTasks[T any](maxTasks byte) option[T] {
	return func(opts *options[T]) {
		opts.agOpts = append(opts.agOpts, aggregate.WithSnapshotMaxTasks[T](maxTasks))
	}
}

func WithDeduplication[T any](duration time.Duration) option[T] {
	return func(opts *options[T]) {
		opts.esCfg = append(opts.esCfg, natsstream.WithDeduplication(duration))
	}
}

func WithEvent[Event any, Aggregate any, PE aggregate.PtrEvolver[Event, Aggregate]]() option[Aggregate] {
	return func(o *options[Aggregate]) {
		o.agOpts = append(o.agOpts, aggregate.WithEvent[Event, Aggregate, PE]())
	}
}

func WithSnapshotMinInterval[T any](interval time.Duration) option[T] {
	return func(a *options[T]) {
		if interval > snapshot.UpperMinInterval || interval < snapshot.LowerMinInterval {
			return
		}
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotMinInterval[T](interval))
	}
}

// WithSnapshotTimeout sets the timeout for snapshotting. Not less than snapshot.UpperTimeout second and not more than snapshot.LowerTimeout seconds.
func WithSnapshotTimeout[T any](timeout time.Duration) option[T] {
	return func(a *options[T]) {
		if timeout > snapshot.UpperTimeout || timeout < snapshot.LowerTimeout {
			return
		}
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotTimeout[T](timeout))
	}
}

func WithSnapshotEventCount[T any](count uint16) option[T] {
	return func(a *options[T]) {
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotEventCount[T](count))
	}
}

func WithCodec[T any](codec codec.Codec) option[T] {
	return func(a *options[T]) {
		a.agOpts = append(a.agOpts, aggregate.WithCodec[T](codec))
	}
}

func WithLogger[T any](logger logger) option[T] {
	return func(a *options[T]) {
		a.agOpts = append(a.agOpts, aggregate.WithLogger[T](logger))
		a.esCfg = append(a.esCfg, natsstream.WithLogger(logger))
	}
}
