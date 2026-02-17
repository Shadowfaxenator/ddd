package natsaggregate

import (
	"context"
	"fmt"

	"github.com/alekseev-bro/ddd/internal/typeregistry"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/drivers/snapshot/natssnapshot"
	"github.com/alekseev-bro/ddd/pkg/drivers/stream/natsstream"
	"github.com/alekseev-bro/ddd/pkg/stream"
	"github.com/nats-io/nats.go/jetstream"
)

func New[T any, PT aggregate.AggregatePtr[T]](ctx context.Context, js jetstream.JetStream, opts ...option[T]) (*aggregate.Aggregate[T, PT], error) {

	cfg := &options[T]{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.snapshotStoreName == "" {
		cfg.snapshotStoreName = fmt.Sprintf("%s", typeregistry.TypeNameFor[T](typeregistry.WithDelimiter("-")))
	}
	if cfg.streamName == "" {
		cfg.streamName = fmt.Sprintf("%s", typeregistry.TypeNameFor[T](typeregistry.WithDelimiter(":")))
	}

	es, err := natsstream.New(ctx, js, cfg.streamName, cfg.esCfg...)
	if err != nil {
		return nil, fmt.Errorf("stream driver: %w", err)
	}
	ss, err := natssnapshot.New(ctx, js, cfg.snapshotStoreName, cfg.ssCfg...)
	if err != nil {
		return nil, fmt.Errorf("snapshot driver: %w", err)
	}

	return aggregate.New[T, PT](ctx, es, ss, cfg.agOpts...)
}

type saver interface {
	Save(ctx context.Context, aggrID aggregate.ID, expectedSequence uint64, events []any) ([]stream.EventMetadata, error)
}

type subscriber interface {
	Subscribe(ctx context.Context, h stream.EventHandler, opts ...stream.ProjOption) (stream.Drainer, error)
}

type Stream interface {
	subscriber
	saver
}
