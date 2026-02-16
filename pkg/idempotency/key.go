package idempotency

import (
	"context"
)

var Key idempotenceKey = "idempKey"

type idempotenceKey string

func ContextWithKey(ctx context.Context, key int64) context.Context {
	return context.WithValue(ctx, Key, key)
}

func KeyFromContext(ctx context.Context) (int64, bool) {
	val, ok := ctx.Value(Key).(int64)
	return val, ok

}
