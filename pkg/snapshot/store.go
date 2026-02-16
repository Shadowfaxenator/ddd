package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/identity"
)

const (
	DefaultSizeInEvents uint16        = 100
	DefaultMinInterval  time.Duration = time.Second * 1
	LowerMinInterval    time.Duration = time.Second * 1
	UpperMinInterval    time.Duration = time.Minute * 10
	DefaultTimeout      time.Duration = time.Second * 5
	LowerTimeout        time.Duration = time.Second * 1
	UpperTimeout        time.Duration = time.Second * 10
)

type logger interface {
	Error(msg string, args ...any)
}
type Aggregate[T any] struct {
	ID        identity.ID
	Sequence  uint64
	Timestamp time.Time
	Version   uint64
	State     *T
}

type Value struct {
	Body      []byte
	Timestamp time.Time
}

type Snapshot[T any] struct {
	Body      *Aggregate[T]
	Timestamp time.Time
}

type snapshotStore[T any] struct {
	codec  codec.Codec
	ss     Store
	logger logger
}

func NewStore[T any](ss Store, opts ...Option[T]) *snapshotStore[T] {
	s := &snapshotStore[T]{
		codec:  codec.JSON,
		ss:     ss,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *snapshotStore[T]) Save(ctx context.Context, a *Aggregate[T]) error {

	b, err := s.codec.Marshal(a)
	if err != nil {
		return fmt.Errorf("snapshot save: %w", err)
	}
	return s.ss.Save(ctx, a.ID.Int64(), b)
}
func (s *snapshotStore[T]) Load(ctx context.Context, id identity.ID) *Snapshot[T] {

	snap, err := s.ss.Load(ctx, id.Int64())
	if err != nil {
		if errors.Is(err, ErrNoSnapshot) {
			return nil
		}
		s.logger.Error("snapshot load", "error", err)
		return nil
	}
	snapshot := &Snapshot[T]{
		Body:      new(Aggregate[T]),
		Timestamp: snap.Timestamp,
	}

	if err := s.codec.Unmarshal(snap.Body, snapshot.Body); err != nil {
		s.logger.Error("snapshot load", "error", err)
		return nil
	}

	return snapshot
}

type Store interface {
	Save(ctx context.Context, key int64, value []byte) error
	Load(ctx context.Context, key int64) (*Value, error)
}
