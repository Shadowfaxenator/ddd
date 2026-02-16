package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
)

type Logger interface {
	Error(msg string, args ...any)
}

type Value struct {
	Body      []byte
	Timestamp time.Time
}

type Snapshot[T any] struct {
	Body      *aggregate.Aggregate[T]
	Timestamp time.Time
}

type store[T any] struct {
	codec  codec.Codec
	ss     Driver
	logger Logger
}

func NewStore[T any](ss Driver, opts ...Option[T]) *store[T] {
	s := &store[T]{
		codec:  codec.JSON,
		ss:     ss,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *store[T]) Save(ctx context.Context, a *aggregate.Aggregate[T]) error {

	b, err := s.codec.Marshal(a)
	if err != nil {
		return fmt.Errorf("snapshot save: %w", err)
	}
	return s.ss.Save(ctx, a.ID.I64(), b)
}
func (s *store[T]) Load(ctx context.Context, id aggregate.ID) (*Snapshot[T], bool) {

	snap, err := s.ss.Load(ctx, id.I64())
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return nil, false
		}
		s.logger.Error("snapshot load", "error", err)
		return nil, false
	}
	snapshot := &Snapshot[T]{
		Body:      new(aggregate.Aggregate[T]),
		Timestamp: snap.Timestamp,
	}

	if err := s.codec.Unmarshal(snap.Body, snapshot.Body); err != nil {
		s.logger.Error("snapshot load", "error", err)
		return nil, false
	}

	return snapshot, true
}

type Driver interface {
	Save(ctx context.Context, key int64, value []byte) error
	Load(ctx context.Context, key int64) (*Value, error)
}
