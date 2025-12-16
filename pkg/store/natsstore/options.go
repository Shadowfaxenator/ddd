package natsstore

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/esnats"
	"github.com/alekseev-bro/ddd/pkg/store/natsstore/snapnats"
)

type options[T aggregate.Aggregatable] struct {
	esOpts []esnats.Option[T]
	ssOpts []snapnats.Option[T]
	agOpts []aggregate.Option[T]
}

type option[T aggregate.Aggregatable] func(*options[T])

func WithInMemory[T aggregate.Aggregatable]() option[T] {
	return func(opts *options[T]) {
		opts.ssOpts = append(opts.ssOpts, snapnats.WithInMemory[T]())
		opts.esOpts = append(opts.esOpts, esnats.WithInMemory[T]())
	}
}

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T aggregate.Aggregatable](numMsgs byte, interval time.Duration) option[T] {
	return func(o *options[T]) {
		o.agOpts = append(o.agOpts, aggregate.WithSnapshotThreshold[T](numMsgs, interval))
	}
}

func WithEvent[E aggregate.Event[T], T aggregate.Aggregatable]() option[T] {
	return func(o *options[T]) {
		o.agOpts = append(o.agOpts, aggregate.WithEvent[E, T]())
	}
}
