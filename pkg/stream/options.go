package stream

import (
	"fmt"
	"reflect"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typeregistry"
	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Option func(*stream) error

func WithEvent[E any]() Option {
	return func(a *stream) error {
		t := reflect.TypeFor[E]()
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("event '%s' must be a struct and not a pointer", t)
		}

		name := typeregistry.TypeNameFor[E](typeregistry.WithPlainTextPkg())
		return a.reg.Register(name, func() any { return new(E) })
	}
}

func WithCodec(c codec.Codec) Option {
	return func(a *stream) error {
		a.eventSerder = serde.NewSerder[any](a.reg, c)
		return nil
	}
}

func WithLogger(logger logger) Option {
	return func(a *stream) error {
		a.logger = logger
		return nil
	}
}
