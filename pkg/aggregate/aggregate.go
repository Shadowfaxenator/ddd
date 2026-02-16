package aggregate

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"reflect"
	"time"

	"github.com/alekseev-bro/ddd/internal/typeregistry"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/identity"
	"github.com/alekseev-bro/ddd/pkg/stream"

	"github.com/alekseev-bro/ddd/pkg/snapshot"
)

type messageCount uint

// Aggregate store type it implements the Aggregate interface.
type StatePtr[T any] interface {
	*T
}

type snapshotStore[T any] interface {
	Save(ctx context.Context, a *snapshot.Aggregate[T]) error
	Load(ctx context.Context, id identity.ID) *snapshot.Snapshot[T]
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T any, PT StatePtr[T]](ctx context.Context, es stream.Store, ss snapshot.Store, opts ...Option[T, PT]) *Aggregate[T, PT] {
	opt := new(storeOptions[T, PT])
	opt.storeConfig = storeConfig{
		SnapshotMsgThreshold: snapshot.DefaultSizeInEvents,
		SnapshotMinInterval:  snapshot.DefaultMinInterval,
		SnapshotTimeout:      snapshot.DefaultTimeout,
		Logger:               slog.Default(),
	}
	for _, o := range opts {
		o(opt)
	}
	str := stream.New(ctx, es, opt.streamOptions...)
	snap := snapshot.NewStore[T](ss, opt.snapshotOptions...)

	aggr := &Aggregate[T, PT]{
		storeConfig: opt.storeConfig,
		es:          str,
		ss:          snap,
		eventTypes:  str,
	}

	//	var zero T

	return aggr
}

// Subscriber is an interface that defines the Project method for projecting events on an
// Events must implement the Event interface.
type Subscriber[T any] interface {
	Subscribe(ctx context.Context, h EventsHandler[T], opts ...stream.ProjOption) (stream.Drainer, error)
}
type eventKindSubscriber[T any] interface {
	Subscriber[T]
	EventKind(in Evolver[T]) (string, error)
}

type CommandHandler[T any, C any] interface {
	HandleCommand(ctx context.Context, cmd C) ([]stream.EventMetadata, error)
}

type EventsHandler[T any] interface {
	HandleEvents(ctx context.Context, event Evolver[T]) error
}

type EventHandler[T any, E Evolver[T]] interface {
	HandleEvent(ctx context.Context, event E) error
}

type eventStream interface {
	LoadEvents(ctx context.Context, id identity.ID, seq uint64) iter.Seq2[*stream.Event, error]
	SaveEvents(ctx context.Context, id identity.ID, expectedSequence uint64, events []any) ([]stream.EventMetadata, error)
	Subscribe(ctx context.Context, h stream.EventHandler, opts ...stream.ProjOption) (stream.Drainer, error)
}

// Mutator is an interface that defines the Update method for executing commands on an
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Executer interface.
type Mutator[T any, PT StatePtr[T]] interface {
	Mutate(ctx context.Context, id ID, modify func(state PT) (Events[T], error)) ([]stream.EventMetadata, error)
}

type eventKinder interface {
	EventKind(in any) (string, error)
}

func (a *Aggregate[T, PT]) logger() logger {
	return a.storeConfig.Logger
}

type Aggregate[T any, PT StatePtr[T]] struct {
	storeConfig storeConfig
	es          eventStream
	ss          snapshotStore[T]
	eventTypes  eventKinder
	codec       codec.Codec
}

func (a *Aggregate[T, PT]) EventKind(in Evolver[T]) (string, error) {
	return a.eventTypes.EventKind(in)
}

func (a *Aggregate[T, PT]) build(ctx context.Context, id ID, sn *snapshot.Snapshot[T]) (*snapshot.Aggregate[T], error) {
	aggr := &snapshot.Aggregate[T]{
		State: PT(new(T)),
	}

	if sn != nil {
		aggr = sn.Body
	}

	events := a.es.LoadEvents(ctx, identity.ID(id), aggr.Sequence)
	for event, err := range events {
		if err != nil {
			if errors.Is(err, ErrNotExists) {
				if sn == nil {
					return nil, nil
				}
				return sn.Body, nil
			} else {
				return nil, fmt.Errorf("buid %w", err)
			}
		}

		if ev, ok := event.Body.(Evolver[T]); ok {
			ev.Evolve(aggr.State)
			aggr.Sequence = event.Sequence
			aggr.Version++
		} else {
			return nil, fmt.Errorf("buid, event is not Evolver[T] %w", err)
		}

	}

	return aggr, nil
}

func (a *Aggregate[T, PT]) Subscribe(ctx context.Context, h EventsHandler[T], opts ...stream.ProjOption) (stream.Drainer, error) {
	var op []stream.ProjOption

	op = append(op, stream.WithName(typeregistry.TypeNameFrom(h)))
	op = append(op, opts...)
	return a.es.Subscribe(ctx, &subscribeHandlerAdapter[T]{h: h}, op...)
}

// Update executes a command on the aggregate root.
func (a *Aggregate[T, PT]) Mutate(
	ctx context.Context, id ID,
	modify func(state PT) (Events[T], error),

) ([]stream.EventMetadata, error) {
	var (
		err                       error
		invError                  error
		aggr                      *snapshot.Aggregate[T]
		notSnaphottedEventsNumber uint64
		numEventsBeforeBuild      uint64
	)
	sn := a.ss.Load(ctx, identity.ID(id))
	if sn != nil {
		numEventsBeforeBuild = sn.Body.Version
	}
	aggr, err = a.build(ctx, id, sn)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	//	}
	var evts Events[T]
	var expVersion uint64
	if aggr != nil {
		evts, err = modify(aggr.State)
		expVersion = aggr.Sequence
	} else {
		evts, err = modify(new(T))
	}
	if err != nil {
		invError = stream.NonRetriableError{Err: err}
	}
	if evts == nil {
		return nil, invError
	}
	numevents := len(evts)
	msgs := make([]any, numevents)
	for i, ev := range evts {
		msgs[i] = ev
	}

	storedMsgs, err := a.es.SaveEvents(ctx, identity.ID(id), expVersion, msgs)
	if err != nil {
		return nil, fmt.Errorf("update save: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if aggr != nil {
		var snapTime time.Time
		if sn != nil {
			snapTime = sn.Timestamp
		}
		aggr.Version += uint64(numevents)
		notSnaphottedEventsNumber = aggr.Version - numEventsBeforeBuild
		if notSnaphottedEventsNumber >= uint64(a.storeConfig.SnapshotMsgThreshold) && time.Since(snapTime) > a.storeConfig.SnapshotMinInterval {
			evts.Evolve(aggr.State)
			if len(storedMsgs) > 0 {
				aggr.Sequence = storedMsgs[len(storedMsgs)-1].Sequence
			}

			go func() {
				ctxSnap, cancel := context.WithTimeout(context.Background(), a.storeConfig.SnapshotTimeout)
				defer cancel()
				if err := a.ss.Save(ctxSnap, aggr); err != nil {
					a.logger().Error("snapshot save", "error", err.Error())
					return
				}

				a.logger().Info("snapshot saved", "sequence", aggr.Sequence, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name(), "timestamp", aggr.Timestamp)

			}()

		}
	}

	return storedMsgs, invError
}

type handleEventAdapter[E Evolver[T], T any] struct {
	h EventHandler[T, E]
}

func (h *handleEventAdapter[E, T]) HandleEvents(ctx context.Context, event Evolver[T]) error {
	return h.h.HandleEvent(ctx, event.(E))
}

type subscribeHandlerAdapter[T any] struct {
	h EventsHandler[T]
}

func (s *subscribeHandlerAdapter[T]) HandleEvents(ctx context.Context, ev any) error {
	if e, ok := ev.(Evolver[T]); ok {
		return s.h.HandleEvents(ctx, e)

	}

	return fmt.Errorf("sub addapter event handler: %w", errors.New("event is not Evolver[T]"))

}

func ProjectEvent[E Evolver[T], T any](ctx context.Context, sub eventKindSubscriber[T], h EventHandler[T, E]) (stream.Drainer, error) {

	var zero any
	t := reflect.TypeFor[E]()
	switch t.Kind() {
	case reflect.Struct:
		zero = new(E)
	case reflect.Pointer:
		var z E
		zero = z
	default:
		return nil, fmt.Errorf("unsupported event type: %s", t)
	}

	n := fmt.Sprintf("%s", typeregistry.TypeNameFrom(h))

	eventKind, err := sub.EventKind(zero.(E))

	if err != nil {
		return nil, fmt.Errorf("project: %w", err)
	}
	return sub.Subscribe(ctx, &handleEventAdapter[E, T]{h: h}, stream.WithFilterByEvent(eventKind), stream.WithName(n))

}
