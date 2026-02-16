package natsstream

import (
	"time"
)

type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type eventStreamConfig struct {
	StoreType     StoreType
	PartitionNum  byte
	Deduplication time.Duration
	Logger        Logger
}

type Option func(*eventStreamConfig)

// func WithPartitions(partitions byte) Option {
// 	return func(es *EventStreamConfig) {
// 		es.PartitionNum = partitions

// 	}
// }

func WithStoreType(storeType StoreType) Option {
	return func(es *eventStreamConfig) {
		es.StoreType = storeType

	}
}

func WithLogger(logger Logger) Option {
	return func(es *eventStreamConfig) {
		es.Logger = logger

	}
}

func WithDeduplication(deduplication time.Duration) Option {
	return func(es *eventStreamConfig) {
		es.Deduplication = deduplication

	}
}
