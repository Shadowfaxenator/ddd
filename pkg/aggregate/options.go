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
type storeOptions[Aggregate any] struct {
	streamOptions   []stream.Option
	snapshotOptions []snapshot.Option[Aggregate]
	storeConfig
}

type Option[Aggregate any] func(a *storeOptions[Aggregate]) error

type PtrEvolver[Event any, Aggregate any] interface {
	*Event
	Evolver[Aggregate]
}

func WithEvent[Event any, Aggregate any, PE PtrEvolver[Event, Aggregate]]() Option[Aggregate] {

	return func(a *storeOptions[Aggregate]) error {

		a.streamOptions = append(a.streamOptions, stream.WithEvent[Event]())

		return nil
	}
}

// WithSnapshotMinInterval sets the minimum interval between snapshots. Not less than snapshot.UpperMinInterval second and not more than snapshot.LowerMinInterval minutes.
func WithSnapshotMinInterval[Aggregate any](interval time.Duration) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		if interval > snapshot.UpperMinInterval || interval < snapshot.LowerMinInterval {
			return errors.New("invalid interval")
		}
		a.storeConfig.SnapshotMinInterval = interval
		return nil
	}
}

// WithSnapshotTimeout sets the timeout for snapshotting. Not less than snapshot.UpperTimeout second and not more than snapshot.LowerTimeout seconds.
func WithSnapshotTimeout[Aggregate any](timeout time.Duration) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		if timeout > snapshot.UpperTimeout || timeout < snapshot.LowerTimeout {
			return errors.New("invalid timeout")
		}
		a.storeConfig.SnapshotTimeout = timeout
		return nil
	}
}

func WithSnapshotEventCount[Aggregate any](count uint16) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		a.storeConfig.SnapshotMsgThreshold = count
		return nil
	}
}

func WithCodec[Aggregate any](codec codec.Codec) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		a.snapshotOptions = append(a.snapshotOptions, snapshot.WithCodec[Aggregate](codec))
		a.streamOptions = append(a.streamOptions, stream.WithCodec(codec))
		return nil
	}
}

func WithLogger[Aggregate any](logger logger) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		a.storeConfig.Logger = logger
		return nil
	}
}

func WithSnapshotMaxTasks[Aggregate any](maxTasks byte) Option[Aggregate] {
	return func(a *storeOptions[Aggregate]) error {
		a.storeConfig.SnapshotMaxTasks = maxTasks
		return nil
	}
}
