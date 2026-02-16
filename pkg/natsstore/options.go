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

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type options[T any, PT eventstore.PRoot[T]] struct {
	esCfg  esnats.EventStreamConfig
	ssCfg  snapnats.SnapshotStoreConfig
	agOpts []eventstore.StoreOption[T, PT]
}

type option[T any, PT eventstore.PRoot[T]] func(c *options[T, PT])

func WithInMemory[T any, PT eventstore.PRoot[T]]() option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg.StoreType = esnats.Memory
		opts.ssCfg.StoreType = snapnats.Memory
	}
}

func WithDeduplication[T any, PT eventstore.PRoot[T]](duration time.Duration) option[T, PT] {
	return func(opts *options[T, PT]) {
		opts.esCfg.Deduplication = duration
	}
}

func WithEvent[E any, T any, PE interface {
	*E
	aggregate.Evolver[T]
}, PT eventstore.PRoot[T]](name string) option[T, PT] {
	return func(o *options[T, PT]) {
		o.agOpts = append(o.agOpts, eventstore.WithEvent[E, T, PE, PT](name))
	}
}

func WithSnapshot[T any, PT eventstore.PRoot[T]](maxMsgs byte, maxInterval time.Duration, timeout time.Duration) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, eventstore.WithSnapshot[T, PT](maxMsgs, maxInterval, timeout))
	}
}

// func WithEventCodec[T any, PT aggregate.PRoot[T]](codec codec.Codec) option[T, PT] {
// 	return func(a *options[T, PT]) {
// 		a.agOpts = append(a.agOpts, aggregate.WithEventCodec[T, PT](codec))
// 	}
// }

func WithSnapshotCodec[T any, PT eventstore.PRoot[T]](codec codec.Codec) option[T, PT] {
	return func(a *options[T, PT]) {
		a.agOpts = append(a.agOpts, eventstore.WithCodec[T, PT](codec))
	}
}
