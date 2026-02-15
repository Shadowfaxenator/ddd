package snapnats

type snapshotStoreConfig struct {
	StoreType StoreType
}

type Option func(*snapshotStoreConfig)

func WithStoreType(storeType StoreType) Option {
	return func(ss *snapshotStoreConfig) {
		ss.StoreType = storeType
	}
}
