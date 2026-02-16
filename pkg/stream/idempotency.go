package stream

import "context"

var iKey idempotenceKey = "idempKey"

type idempotenceKey string

func ContextWithIdempotencyKey(ctx context.Context, key int64) context.Context {
	return context.WithValue(ctx, iKey, key)
}

func IdempotencyKeyFromContext(ctx context.Context) (int64, bool) {
	val, ok := ctx.Value(iKey).(int64)
	return val, ok

}
