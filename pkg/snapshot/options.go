package snapshot

import (
	"log/slog"

	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Option[T any] func(*store[T])

func WithCodec[T any](codec codec.Codec) Option[T] {
	return func(s *store[T]) {
		s.codec = codec
	}
}

func WithLogger[T any](logger *slog.Logger) Option[T] {
	return func(s *store[T]) {
		s.logger = logger
	}
}
