package stream

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Option func(*stream) error

func WithEvent[E any](name string) Option {
	return func(a *stream) error {
		if reflect.TypeFor[E]().Kind() != reflect.Struct {
			return fmt.Errorf("event '%s' must be a struct and not a pointer", name)
		}
		if name == "" {
			name = reflect.TypeFor[E]().Name()
		}
		name = strings.ReplaceAll(name, ".", "")

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
