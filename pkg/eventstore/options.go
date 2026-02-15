package eventstore

import (
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/snapshot"
	"github.com/alekseev-bro/ddd/pkg/stream"
)

type storeConfig struct {
	SnapshotMsgThreshold byte
	SnapshotMaxInterval  time.Duration
	SnapshotTimeout      time.Duration
	Logger               *slog.Logger
}

// func (o StoreOption[T, PT]) ToStreamOption() stream.Option{

// }
type storeOptions[T any, PT PtrAggr[T]] struct {
	streamOptions   []stream.Option
	snapshotOptions []snapshot.Option[T]
	storeConfig
}

type StoreOption[T any, PT PtrAggr[T]] func(a *storeOptions[T, PT])

func WithEvent[E any, T any, PE interface {
	*E
	aggregate.Evolver[T]
}, PT PtrAggr[T]](name string) StoreOption[T, PT] {

	if reflect.TypeFor[E]().Kind() != reflect.Struct {
		panic(fmt.Sprintf("event '%s' must be a struct and not a pointer", name))
	}
	return func(a *storeOptions[T, PT]) {

		a.streamOptions = append(a.streamOptions, stream.WithEvent[E](name))
	}
}

func WithSnapshot[T any, PT PtrAggr[T]](maxMsgs byte, maxInterval time.Duration, timeout time.Duration) StoreOption[T, PT] {
	return func(a *storeOptions[T, PT]) {
		a.storeConfig = storeConfig{
			SnapshotMsgThreshold: maxMsgs,
			SnapshotMaxInterval:  maxInterval,
			SnapshotTimeout:      timeout,
		}
	}
}

func WithCodec[T any, PT PtrAggr[T]](codec codec.Codec) StoreOption[T, PT] {
	return func(a *storeOptions[T, PT]) {
		a.snapshotOptions = append(a.snapshotOptions, snapshot.WithCodec[T](codec))
		a.streamOptions = append(a.streamOptions, stream.WithCodec(codec))
	}
}

func WithLogger[T any, PT PtrAggr[T]](logger *slog.Logger) StoreOption[T, PT] {
	return func(a *storeOptions[T, PT]) {
		a.storeConfig.Logger = logger
	}
}
