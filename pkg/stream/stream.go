package stream

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"

	"github.com/alekseev-bro/ddd/pkg/idempotency"
)

var ErrNonRetriable = errors.New("non-retriable error")

type InfoErrorer interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

type Driver interface {
	Save(ctx context.Context, aggrID int64, expectedSequence uint64, msgs []Msg, idempotancyKey int64) ([]MsgMetadata, error)
	Load(ctx context.Context, aggrID int64, fromSeq uint64, h func(*StoredMsg) error) error
	subscriber
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(msg *StoredMsg) error, params *SubscribeParams) (Drainer, error)
}

type eventSerder serde.Serder[any]

type stream struct {
	name  string
	store Driver
	typereg.TypeRegistry
	eventSerder eventSerder
	logger      InfoErrorer
}

func New(ctx context.Context, sub Driver, opts ...Option) *stream {
	reg := typereg.New()
	ser := serde.NewSerder[any](reg, codec.JSON)
	st := &stream{
		store:        sub,
		TypeRegistry: reg,
		eventSerder:  ser,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(st)
	}
	return st
}

type nameForer interface {
	Kind(in any) (string, error)
}

type EventHandler interface {
	HandleEvents(ctx context.Context, e any) error
}
type Msg struct {
	ID   int64
	Body []byte
	Kind string
}

type MsgMetadata struct {
	MsgID    int64
	Sequence uint64
}

type StoredMsg struct {
	ID        int64
	Body      []byte
	Kind      string
	Sequence  uint64
	Timestamp time.Time
}

type Event struct {
	ID        aggregate.EventID
	Body      any
	Kind      string
	Sequence  uint64
	Timestamp time.Time
}

func (s *stream) Save(ctx context.Context, aggrID aggregate.ID, expectedSequence uint64, events []any) ([]MsgMetadata, error) {

	var msgs []Msg
	for _, ev := range events {
		b, err := s.eventSerder.Serialize(ev)
		if err != nil {
			return nil, fmt.Errorf("save %w", err)
		}
		kind, err := s.Kind(ev)
		if err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}

		msgs = append(msgs, Msg{ID: int64(aggregate.NewEventID()), Body: b, Kind: kind})
	}
	var idemp int64
	if i, ok := idempotency.KeyFromContext(ctx); ok {
		idemp = i
	}
	smsgs, err := s.store.Save(ctx, int64(aggrID), expectedSequence, msgs, idemp)
	if err != nil {
		return nil, fmt.Errorf("save %w", err)
	}

	return smsgs, nil
}

func (s *stream) Load(ctx context.Context, id aggregate.ID, seq uint64) iter.Seq2[*Event, error] {
	var errStop = errors.New("load iterator stopped")
	return func(yield func(*Event, error) bool) {

		if err := s.store.Load(ctx, int64(id), seq, func(sm *StoredMsg) error {

			ev, err := s.eventSerder.Deserialize(sm.Kind, sm.Body)
			if err != nil {
				if !yield(nil, fmt.Errorf("deserialize error: %w", err)) {
					return errStop
				}
				return nil
			}
			e := &Event{
				ID:        aggregate.EventID(sm.ID),
				Body:      ev,
				Kind:      sm.Kind,
				Sequence:  sm.Sequence,
				Timestamp: sm.Timestamp,
			}
			if !yield(e, nil) {
				return errStop
			}
			return nil
		}); err != nil && !errors.Is(err, errStop) {
			yield(nil, fmt.Errorf("store load error: %w", err))
		}
	}

}

type Drainer interface {
	Drain() error
}

// Subscribe creates a new subscription on aggegate events with the given handler.
func (a *stream) Subscribe(ctx context.Context, h EventHandler, opts ...ProjOption) (Drainer, error) {
	dn := typereg.TypeNameFrom(h)

	params := &SubscribeParams{
		DurableName: dn,
		Reg:         a.TypeRegistry,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(params)
		}
	}

	d, err := a.store.Subscribe(ctx, func(msg *StoredMsg) error {

		ev, err := a.eventSerder.Deserialize(msg.Kind, msg.Body)
		if err != nil {
			a.logger.Error("can't deserialize event", "kind", msg.Kind)
			return ErrNonRetriable
		}
		return h.HandleEvents(idempotency.ContextWithKey(ctx, msg.ID), ev)
	}, params)
	if err == nil {
		a.logger.Info("subscription created", "subscription", params.DurableName)
	}
	return d, nil
}
