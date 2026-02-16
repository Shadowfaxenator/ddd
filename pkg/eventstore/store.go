package eventstore

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"reflect"
	"time"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/codec"
	"github.com/alekseev-bro/ddd/pkg/stream"

	"github.com/alekseev-bro/ddd/pkg/snapshot"
)

type messageCount uint

const (
	snapshotSize      messageCount  = 100
	snapshotInterval  time.Duration = time.Second * 1
	idempotancyWindow time.Duration = time.Minute * 2
	snapshotTimeout   time.Duration = time.Second * 5
)

type ComparableStringer interface {
	fmt.Stringer
	comparable
}

type EventSerder[T any] interface {
	Serialize(aggregate.Evolver[T]) ([]byte, error)
	Deserialize(string, []byte) (aggregate.Evolver[T], error)
}

// Aggregate store type it implements the Aggregate interface.
type PRoot[T any] interface {
	*T
}

type snapshotStore[T any] interface {
	Save(ctx context.Context, a *aggregate.Aggregate[T]) error
	Load(ctx context.Context, id aggregate.ID) (*snapshot.Snapshot[T], bool)
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T any, PT PRoot[T]](ctx context.Context, es stream.Driver, ss snapshot.Driver, opts ...StoreOption[T, PT]) *Store[T, PT] {
	opt := new(storeOptions[T, PT])
	opt.storeConfig = storeConfig{
		SnapthotMsgThreshold: byte(snapshotSize),
		SnapshotMaxInterval:  snapshotInterval,
		SnapshotTimeout:      snapshotTimeout,
	}
	for _, o := range opts {
		o(opt)
	}
	str := stream.New(ctx, es, opt.streamOptions...)
	snap := snapshot.NewStore[T](ss, opt.snapshotOptions...)

	aggr := &Store[T, PT]{
		storeConfig: opt.storeConfig,
		es:          str,
		ss:          snap,
		eventTypes:  str,
	}

	//	var zero T

	return aggr
}

// Subscriber is an interface that defines the Project method for projecting events on an aggregate.
// Events must implement the Event interface.
type Subscriber[T any] interface {
	Subscribe(ctx context.Context, h EventsHandler[T], opts ...stream.ProjOption) (stream.Drainer, error)
}
type EventKindSubscriber[T any] interface {
	Subscriber[T]
	EventKind(in aggregate.Evolver[T]) (string, error)
}

type CommandHandler[T any, C any] interface {
	HandleCommand(ctx context.Context, cmd C) ([]stream.MsgMetadata, error)
}

type EventsHandler[T any] interface {
	HandleEvents(ctx context.Context, event aggregate.Evolver[T]) error
}

type EventHandler[T any, E aggregate.Evolver[T]] interface {
	HandleEvent(ctx context.Context, event E) error
}

type eventStream interface {
	Load(ctx context.Context, id aggregate.ID, seq uint64) iter.Seq2[*stream.Event, error]
	Save(ctx context.Context, aggrID aggregate.ID, expectedSequence uint64, events []any) ([]stream.MsgMetadata, error)
	Subscribe(ctx context.Context, h stream.EventHandler, opts ...stream.ProjOption) (stream.Drainer, error)
}

// Mutator is an interface that defines the Update method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Executer interface.
type Mutator[T any, PT PRoot[T]] interface {
	Mutate(ctx context.Context, id aggregate.ID, modify func(state PT) (aggregate.Events[T], error)) ([]stream.MsgMetadata, error)
}

type kinder interface {
	Kind(in any) (string, error)
}

type Store[T any, PT PRoot[T]] struct {
	storeConfig
	es         eventStream
	ss         snapshotStore[T]
	eventTypes kinder
	codec      codec.Codec
}

func (a *Store[T, PT]) EventKind(in aggregate.Evolver[T]) (string, error) {
	return a.eventTypes.Kind(in)
}

func (a *Store[T, PT]) build(ctx context.Context, id aggregate.ID, sn *snapshot.Snapshot[T]) (*aggregate.Aggregate[T], error) {
	aggr := &aggregate.Aggregate[T]{
		State: PT(new(T)),
	}

	if sn != nil {
		aggr = sn.Body
	}

	events := a.es.Load(ctx, id, aggr.Sequence)
	for event, err := range events {
		if err != nil {
			if errors.Is(err, ErrNoAggregate) {
				if sn == nil {
					return nil, nil
				}
			} else {
				return nil, fmt.Errorf("buid %w", err)
			}
		}

		if ev, ok := event.Body.(aggregate.Evolver[T]); ok {
			ev.Evolve(aggr.State)
			aggr.Sequence = event.Sequence
			aggr.Version++
		} else {
			return nil, fmt.Errorf("buid, event is not Evolver[T] %w", err)
		}

	}

	return aggr, nil
}

func (a *Store[T, PT]) Subscribe(ctx context.Context, h EventsHandler[T], opts ...stream.ProjOption) (stream.Drainer, error) {
	var op []stream.ProjOption

	op = append(op, stream.WithName(typereg.TypeNameFrom(h)))
	op = append(op, opts...)
	return a.es.Subscribe(ctx, &subscribeHandlerAddapter[T]{h: h}, op...)
}

// Update executes a command on the aggregate root.
func (a *Store[T, PT]) Mutate(
	ctx context.Context, id aggregate.ID,
	modify func(state PT) (aggregate.Events[T], error),

) ([]stream.MsgMetadata, error) {
	var (
		err                       error
		invError                  error
		aggr                      *aggregate.Aggregate[T]
		notSnaphottedEventsNumber uint64
		numEventsBeforeBuild      uint64
	)
	sn, hasSnapshot := a.ss.Load(ctx, id)
	if hasSnapshot {
		numEventsBeforeBuild = sn.Body.Version
	}
	aggr, err = a.build(ctx, id, sn)
	if err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}
	//	}
	var evts aggregate.Events[T]
	var expVersion uint64
	if aggr != nil {
		evts, err = modify(aggr.State)
		expVersion = aggr.Sequence
	} else {
		evts, err = modify(new(T))
	}
	if err != nil {
		invError = &aggregate.InvariantViolationError{Err: err}
	}
	if evts == nil {
		return nil, invError
	}
	numevents := len(evts)
	msgs := make([]any, numevents)
	for i, ev := range evts {
		msgs[i] = ev
	}

	storedMsgs, err := a.es.Save(ctx, id, expVersion, msgs)
	if err != nil {
		return nil, fmt.Errorf("update save: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if aggr != nil {
		var snapTime time.Time
		if hasSnapshot {
			snapTime = sn.Timestamp
		}
		aggr.Version += uint64(numevents)
		notSnaphottedEventsNumber = aggr.Version - numEventsBeforeBuild
		if notSnaphottedEventsNumber >= uint64(a.SnapthotMsgThreshold) && time.Since(snapTime) > a.SnapshotMaxInterval {
			evts.Evolve(aggr.State)
			if len(storedMsgs) > 0 {
				aggr.Sequence = storedMsgs[len(storedMsgs)-1].Sequence
			}

			go func() {
				ctxSnap, cancel := context.WithTimeout(context.Background(), a.SnapshotTimeout)
				defer cancel()
				if err := a.ss.Save(ctxSnap, aggr); err != nil {
					slog.Error("snapshot save", "error", err.Error())
					return
				}

				slog.Info("snapshot saved", "sequence", aggr.Sequence, "aggregateID", id.String(), "aggregate", reflect.TypeFor[T]().Name(), "timestamp", aggr.Timestamp)

			}()

		}
	}

	return storedMsgs, invError
}

type handleEventAdapter[E aggregate.Evolver[T], T any] struct {
	h EventHandler[T, E]
}

func (h *handleEventAdapter[E, T]) HandleEvents(ctx context.Context, event aggregate.Evolver[T]) error {
	return h.h.HandleEvent(ctx, event.(E))
}

type subscribeHandlerAddapter[T any] struct {
	h EventsHandler[T]
}

func (s *subscribeHandlerAddapter[T]) HandleEvents(ctx context.Context, ev any) error {
	if e, ok := ev.(aggregate.Evolver[T]); ok {
		return s.h.HandleEvents(ctx, e)

	}

	return fmt.Errorf("sub addapter event handler: %w", errors.New("event is not Evolver[T]"))

}

func Project[E aggregate.Evolver[T], T any](ctx context.Context, sub EventKindSubscriber[T], h EventHandler[T, E]) (stream.Drainer, error) {

	var zero any
	t := reflect.TypeFor[E]()
	switch t.Kind() {
	case reflect.Struct:
		zero = new(E)
	case reflect.Pointer:
		var z E
		zero = z
	default:
		panic(fmt.Sprintf("unsupported event type: %s", t))
	}

	n := fmt.Sprintf("%s", typereg.TypeNameFrom(h))

	eventKind, err := sub.EventKind(zero.(E))

	if err != nil {
		return nil, fmt.Errorf("project: %w", err)
	}
	return sub.Subscribe(ctx, &handleEventAdapter[E, T]{h: h}, stream.WithFilterByEvent(eventKind), stream.WithName(n))

}
