package aggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/internal/typereg"
)

type Option[T Aggregatable] func(a *Root[T])

// WithSnapshotThreshold sets the threshold for snapshotting.
// numMsgs is the number of messages to accumulate before snapshotting,
// and the interval is the minimum time interval between snapshots.
func WithSnapshotThreshold[T Aggregatable](numMsgs byte, interval time.Duration) Option[T] {
	return func(a *Root[T]) {
		a.snapshotThreshold.numMsgs = numMsgs
		a.snapshotThreshold.interval = interval
	}
}

func WithEvent[E Event[T], T Aggregatable]() Option[T] {
	return func(a *Root[T]) {
		var zero E
		typereg.Register(zero)
	}
}
