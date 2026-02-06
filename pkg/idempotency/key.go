package idempotency

import (
	"context"

	"github.com/alekseev-bro/ddd/pkg/id"
)

var Key idempotanceKey = "idempKey"

type idempotanceKey string

func KeyFromCtxOrRandom(ctx context.Context) int64 {
	if val, ok := ctx.Value(Key).(int64); ok {
		return val
	}
	return id.New()
}

func ContextWithKey(ctx context.Context, key int64) context.Context {
	return context.WithValue(ctx, Key, key)
}

func KeyFromContext(ctx context.Context) (int64, bool) {
	val, ok := ctx.Value(Key).(int64)
	return val, ok

}
