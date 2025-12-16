package esnats

import "github.com/alekseev-bro/ddd/pkg/aggregate"

type Option[T aggregate.Aggregatable] func(*eventStream[T]) error

func WithPartitions[T aggregate.Aggregatable](partitions byte) Option[T] {
	return func(es *eventStream[T]) error {
		es.partnum = partitions
		return nil
	}
}

func WithInMemory[T aggregate.Aggregatable]() Option[T] {
	return func(es *eventStream[T]) error {
		es.storeType = Memory
		return nil
	}
}
