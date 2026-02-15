package esnats

import (
	"log/slog"
	"time"
)

type eventStreamConfig struct {
	StoreType     StoreType
	PartitionNum  byte
	Deduplication time.Duration
	Logger        *slog.Logger
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

func WithLogger(logger *slog.Logger) Option {
	return func(es *eventStreamConfig) {
		es.Logger = logger

	}
}

func WithDeduplication(deduplication time.Duration) Option {
	return func(es *eventStreamConfig) {
		es.Deduplication = deduplication

	}
}
