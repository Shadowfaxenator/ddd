package stream

import (
	"fmt"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Option func(*stream)

func WithEvent[E any](name string) Option {

	if reflect.TypeFor[E]().Kind() != reflect.Struct {
		panic(fmt.Sprintf("event '%s' must be a struct and not a pointer", name))
	}
	return func(a *stream) {

		if name == "" {
			name = reflect.TypeFor[E]().Name()
		}

		a.Register(name, func() any { return new(E) })
	}
}

func WithCodec(c codec.Codec) Option {
	return func(a *stream) {
		a.eventSerder = serde.NewSerder[any](a.TypeRegistry, c)
	}
}

func WithLogger(logger Logger) Option {
	return func(a *stream) {
		a.logger = logger
	}
}
