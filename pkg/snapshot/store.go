package snapshot

import (
	"context"
	"time"
)

type Aggregate[T any] struct {
	ID        string
	Sequence  uint64
	Timestamp time.Time
	State     T
}

type Snapshot struct {
	Body      []byte
	Timestamp time.Time
}

type Driver interface {
	Save(ctx context.Context, key int64, value []byte) error
	Load(ctx context.Context, key int64) (*Snapshot, error)
}
