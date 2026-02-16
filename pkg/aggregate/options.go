package aggregate

import (
	"time"

	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/snapshot"

	"github.com/alekseev-bro/ddd/pkg/stream"
)

type logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type storeConfig struct {
	SnapshotMsgThreshold uint16
	SnapshotMinInterval  time.Duration
	SnapshotTimeout      time.Duration
	Logger               logger
}

// func (o StoreOption[T, PT]) ToStreamOption() stream.Option{

// }
type storeOptions[T any, PT StatePtr[T]] struct {
	streamOptions   []stream.Option
	snapshotOptions []snapshot.Option[T]
	storeConfig
}

type Option[T any, PT StatePtr[T]] func(a *storeOptions[T, PT])

func WithEvent[E any, T any, PE interface {
	*E
	Evolver[T]
}, PT StatePtr[T]](name string) Option[T, PT] {

	return func(a *storeOptions[T, PT]) {

		a.streamOptions = append(a.streamOptions, stream.WithEvent[E](name))
	}
}

// WithSnapshotMinInterval sets the minimum interval between snapshots. Not less than snapshot.UpperMinInterval second and not more than snapshot.LowerMinInterval minutes.
func WithSnapshotMinInterval[T any, PT StatePtr[T]](interval time.Duration) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {
		if interval > snapshot.UpperMinInterval || interval < snapshot.LowerMinInterval {
			return
		}
		a.storeConfig.SnapshotMinInterval = interval
	}
}

// WithSnapshotTimeout sets the timeout for snapshotting. Not less than snapshot.UpperTimeout second and not more than snapshot.LowerTimeout seconds.
func WithSnapshotTimeout[T any, PT StatePtr[T]](timeout time.Duration) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {
		if timeout > snapshot.UpperTimeout || timeout < snapshot.LowerTimeout {
			return
		}
		a.storeConfig.SnapshotTimeout = timeout
	}
}

func WithSnapshotEventCount[T any, PT StatePtr[T]](count uint16) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {

		a.storeConfig.SnapshotMsgThreshold = count
	}
}

func WithSnapshotMsgThreshold[T any, PT StatePtr[T]](threshold uint16) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {
		if threshold > 10000 || threshold < 1 {
			return
		}
		a.storeConfig.SnapshotMsgThreshold = threshold
	}
}

func WithCodec[T any, PT StatePtr[T]](codec codec.Codec) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {
		a.snapshotOptions = append(a.snapshotOptions, snapshot.WithCodec[T](codec))
		a.streamOptions = append(a.streamOptions, stream.WithCodec(codec))
	}
}

func WithLogger[T any, PT StatePtr[T]](logger logger) Option[T, PT] {
	return func(a *storeOptions[T, PT]) {
		a.storeConfig.Logger = logger
	}
}
