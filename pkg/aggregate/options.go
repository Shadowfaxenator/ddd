package aggregate

import (
	"errors"
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
	SnapshotMaxTasks     byte
	Logger               logger
}

// func (o StoreOption[T, PT]) ToStreamOption() stream.Option{

// }
type storeOptions[T any, PT StatePtr[T]] struct {
	streamOptions   []stream.Option
	snapshotOptions []snapshot.Option[T]
	storeConfig
}

type Option[T any, PT StatePtr[T]] func(a *storeOptions[T, PT]) error

func WithEvent[E any, T any, PE interface {
	*E
	Evolver[T]
}, PT StatePtr[T]]() Option[T, PT] {

	return func(a *storeOptions[T, PT]) error {

		a.streamOptions = append(a.streamOptions, stream.WithEvent[E]())

		return nil
	}
}

// WithSnapshotMinInterval sets the minimum interval between snapshots. Not less than snapshot.UpperMinInterval second and not more than snapshot.LowerMinInterval minutes.
func WithSnapshotMinInterval[T any, PT StatePtr[T]](interval time.Duration) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		if interval > snapshot.UpperMinInterval || interval < snapshot.LowerMinInterval {
			return errors.New("invalid interval")
		}
		a.storeConfig.SnapshotMinInterval = interval
		return nil
	}
}

// WithSnapshotTimeout sets the timeout for snapshotting. Not less than snapshot.UpperTimeout second and not more than snapshot.LowerTimeout seconds.
func WithSnapshotTimeout[T any, PT StatePtr[T]](timeout time.Duration) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		if timeout > snapshot.UpperTimeout || timeout < snapshot.LowerTimeout {
			return errors.New("invalid timeout")
		}
		a.storeConfig.SnapshotTimeout = timeout
		return nil
	}
}

func WithSnapshotEventCount[T any, PT StatePtr[T]](count uint16) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		a.storeConfig.SnapshotMsgThreshold = count
		return nil
	}
}

func WithCodec[T any, PT StatePtr[T]](codec codec.Codec) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		a.snapshotOptions = append(a.snapshotOptions, snapshot.WithCodec[T](codec))
		a.streamOptions = append(a.streamOptions, stream.WithCodec(codec))
		return nil
	}
}

func WithLogger[T any, PT StatePtr[T]](logger logger) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		a.storeConfig.Logger = logger
		return nil
	}
}

func WithSnapshotMaxTasks[T any, PT StatePtr[T]](maxTasks byte) Option[T, PT] {
	return func(a *storeOptions[T, PT]) error {
		a.storeConfig.SnapshotMaxTasks = maxTasks
		return nil
	}
}
