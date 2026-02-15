package snapnats

import "log/slog"

type snapshotStoreConfig struct {
	StoreType StoreType
	Logger    *slog.Logger
}

type Option func(*snapshotStoreConfig)

func WithStoreType(storeType StoreType) Option {
	return func(ss *snapshotStoreConfig) {
		ss.StoreType = storeType
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(ss *snapshotStoreConfig) {
		ss.Logger = logger
	}
}
