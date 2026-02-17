package natssnapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/alekseev-bro/ddd/pkg/snapshot"

	"github.com/nats-io/nats.go/jetstream"
)

type snapshotStore struct {
	*snapshotStoreConfig
	kv jetstream.KeyValue
}

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

func New(ctx context.Context, js jetstream.JetStream, name string, options ...Option) (*snapshotStore, error) {
	cfg := &snapshotStoreConfig{
		StoreType: Disk,
	}
	for _, option := range options {
		option(cfg)
	}

	ss := &snapshotStore{
		snapshotStoreConfig: cfg,
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  ss.snapshotBucketName(name),
		Storage: jetstream.StorageType(ss.StoreType),
	})
	if err != nil {
		return nil, fmt.Errorf("can not create key value: %w", err)
	}

	ss.kv = kv
	return ss, nil
}

func (s *snapshotStore) snapshotBucketName(name string) string {
	return fmt.Sprintf("snapshot-%s", name)
}

func (s *snapshotStore) Save(ctx context.Context, key int64, value []byte) error {

	_, err := s.kv.Put(ctx, strconv.Itoa(int(key)), value)
	return err
}

func (s *snapshotStore) Load(ctx context.Context, key int64) (*snapshot.Value, error) {

	v, err := s.kv.Get(ctx, strconv.Itoa(int(key)))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, snapshot.ErrNoSnapshot
		}
		return nil, err
	}

	snap := &snapshot.Value{
		Body:      v.Value(),
		Timestamp: v.Created(),
	}
	return snap, nil
}
