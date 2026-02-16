package natsaggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/drivers/snapshot/snapnats"
	"github.com/alekseev-bro/ddd/pkg/drivers/stream/esnats"
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

type options[T any, PT aggregate.StatePtr[T]] struct {
	esCfg  []esnats.Option
	ssCfg  []snapnats.Option
	agOpts []aggregate.Option[T, PT]
}

type option[T any, PT aggregate.StatePtr[T]] func(c *options[T, PT])

func WithInMemory[T any, PT aggregate.StatePtr[T]]() option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg = append(opts.esCfg, esnats.WithStoreType(esnats.Memory))
		opts.ssCfg = append(opts.ssCfg, snapnats.WithStoreType(snapnats.Memory))
	}
}

func WithSnapshotMaxTasks[T any, PT aggregate.StatePtr[T]](maxTasks byte) option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.agOpts = append(opts.agOpts, aggregate.WithSnapshotMaxTasks[T, PT](maxTasks))
	}
}

func WithDeduplication[T any, PT aggregate.StatePtr[T]](duration time.Duration) option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg = append(opts.esCfg, esnats.WithDeduplication(duration))
	}
}

func WithEvent[E any, T any, PE interface {
	*E
	aggregate.Evolver[T]
}, PT aggregate.StatePtr[T]](name string) option[T, PT] {
	return func(o *options[T, PT]) {
		o.agOpts = append(o.agOpts, aggregate.WithEvent[E, T, PE, PT](name))
	}
}

func WithSnapshotMinInterval[T any, PT aggregate.StatePtr[T]](interval time.Duration) option[T, PT] {
	return func(a *options[T, PT]) {
		if interval > snapshot.UpperMinInterval || interval < snapshot.LowerMinInterval {
			return
		}
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotMinInterval[T, PT](interval))
	}
}

// WithSnapshotTimeout sets the timeout for snapshotting. Not less than snapshot.UpperTimeout second and not more than snapshot.LowerTimeout seconds.
func WithSnapshotTimeout[T any, PT aggregate.StatePtr[T]](timeout time.Duration) option[T, PT] {
	return func(a *options[T, PT]) {
		if timeout > snapshot.UpperTimeout || timeout < snapshot.LowerTimeout {
			return
		}
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotTimeout[T, PT](timeout))
	}
}

func WithSnapshotEventCount[T any, PT aggregate.StatePtr[T]](count uint16) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithSnapshotEventCount[T, PT](count))
	}
}

func WithCodec[T any, PT aggregate.StatePtr[T]](codec codec.Codec) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithCodec[T, PT](codec))
	}
}

func WithLogger[T any, PT aggregate.StatePtr[T]](logger logger) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, aggregate.WithLogger[T, PT](logger))
		a.esCfg = append(a.esCfg, esnats.WithLogger(logger))
	}
}
