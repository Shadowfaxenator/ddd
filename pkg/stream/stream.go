package stream

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"reflect"
	"time"

	"github.com/alekseev-bro/ddd/internal/prettylog"
	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typeregistry"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/identity"
)

type logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

type Store interface {
	Save(ctx context.Context, aggrID int64, expectedSequence uint64, msgs []Msg, idempotancyKey int64) ([]EventMetadata, error)
	Load(ctx context.Context, aggrID int64, fromSeq uint64, h func(*StoredMsg) error) error
	Subscribe(ctx context.Context, handler func(msg *StoredMsg) error, params *SubscribeParams) (Drainer, error)
}

type eventSerder serde.Serder[any]

type stream struct {
	subs        DrainList
	name        string
	store       Store
	reg         typeregistry.CreateKinderRegistry
	eventSerder eventSerder
	logger      logger
}

func New(sub Store, opts ...Option) (*stream, error) {
	reg := typeregistry.New()
	ser := serde.NewSerder[any](reg, codec.JSON)
	st := &stream{
		store:       sub,
		reg:         reg,
		eventSerder: ser,
		logger:      prettylog.NewDefault(),
	}
	for _, opt := range opts {
		if err := opt(st); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}
	return st, nil
}

type EventHandler interface {
	HandleEvents(ctx context.Context, e any) error
}
type Msg struct {
	ID   int64
	Body []byte
	Kind string
}

type EventMetadata struct {
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
	ID        identity.ID
	Body      any
	Kind      string
	Sequence  uint64
	Timestamp time.Time
}

func (s *stream) EventKind(in any) (string, error) {
	return s.reg.Kind(in)
}

func validatePointerEvent(ev any) error {
	if ev == nil {
		return errors.New("event is nil; expected non-nil pointer to struct")
	}
	v := reflect.ValueOf(ev)
	t := v.Type()
	if t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("invalid event %T: expected pointer to struct (*Event)", ev)
	}
	if v.IsNil() {
		return fmt.Errorf("invalid event %T: nil pointer is not allowed", ev)
	}
	return nil
}

func (s *stream) SaveEvents(ctx context.Context, aggrID identity.ID, expectedSequence uint64, events []any) ([]EventMetadata, error) {

	var msgs []Msg
	for _, ev := range events {

		if err := validatePointerEvent(ev); err != nil {
			return nil, fmt.Errorf("saving events failed: %w", err)
		}

		b, err := s.eventSerder.Serialize(ev)
		if err != nil {
			return nil, fmt.Errorf("saving events failed: %w", err)
		}
		kind, err := s.reg.Kind(ev)
		if err != nil {
			return nil, fmt.Errorf("saving events failed: %w", err)
		}
		evid, err := identity.New()
		if err != nil {
			return nil, fmt.Errorf("saving events failed: %w", err)
		}
		msgs = append(msgs, Msg{ID: evid.Int64(), Body: b, Kind: kind})
	}
	var idemp int64
	if i, ok := IdempotencyKeyFromContext(ctx); ok {
		idemp = i
	}
	smsgs, err := s.store.Save(ctx, int64(aggrID), expectedSequence, msgs, idemp)
	if err != nil {
		return nil, fmt.Errorf("saving events failed: %w", err)
	}

	return smsgs, nil
}

func (s *stream) LoadEvents(ctx context.Context, id identity.ID, seq uint64) iter.Seq2[*Event, error] {
	var errStop = errors.New("load iterator stopped")
	return func(yield func(*Event, error) bool) {

		if err := s.store.Load(ctx, id.Int64(), seq, func(sm *StoredMsg) error {

			ev, err := s.eventSerder.Deserialize(sm.Kind, sm.Body)
			if err != nil {
				if !yield(nil, fmt.Errorf("deserialize error: %w", err)) {
					return errStop
				}
				return nil
			}
			e := &Event{
				ID:        identity.ID(sm.ID),
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

type DrainList []Drainer

func (d DrainList) Drain() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for _, drainer := range d {
		go func(ctx context.Context, drainer Drainer) {
			select {
			case <-ctx.Done():
				return
			default:
				if err := drainer.Drain(); err != nil {
					slog.Error("drain", "error", err)
				}
			}

		}(ctx, drainer)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *stream) Drain() error {
	if err := s.subs.Drain(); err != nil {
		return fmt.Errorf("steam drain: %w", err)
	}

	return nil
}

// Subscribe creates a new subscription on aggegate events with the given handler.
func (a *stream) Subscribe(ctx context.Context, h EventHandler, opts ...ProjOption) error {
	dn := typeregistry.TypeNameFrom(h)

	params := &SubscribeParams{
		DurableName: dn,
		Reg:         a.reg,
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
			return NonRetriableError{err}
		}
		return h.HandleEvents(ContextWithIdempotencyKey(ctx, msg.ID), ev)
	}, params)
	if err != nil {
		return fmt.Errorf("stream subscription failed: %w", err)
	}
	a.subs = append(a.subs, d)
	a.logger.Info("subscription created", "subscription", params.DurableName)
	return nil
}
