package snapnats

type Option[T any] func(*snapshotStore[T])

func WithInMemory[T any]() Option[T] {
	return func(ss *snapshotStore[T]) {
		ss.storeType = Memory

	}
}
