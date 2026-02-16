package stream

import (
	"fmt"
	"reflect"
	"strings"

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
		name = strings.ReplaceAll(name, ".", "")

		a.reg.Register(name, func() any { return new(E) })
	}
}

func WithCodec(c codec.Codec) Option {
	return func(a *stream) {
		a.eventSerder = serde.NewSerder[any](a.reg, c)
	}
}

func WithLogger(logger logger) Option {
	return func(a *stream) {
		a.logger = logger
	}
}
