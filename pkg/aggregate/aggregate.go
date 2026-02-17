package aggregate

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/alekseev-bro/ddd/internal/prettylog"
	"github.com/alekseev-bro/ddd/internal/typeregistry"
	"github.com/alekseev-bro/ddd/pkg/identity"
	"github.com/alekseev-bro/ddd/pkg/snapshot"
	"github.com/alekseev-bro/ddd/pkg/stream"
)

// Aggregate store type it implements the Aggregate interface.
type AggregatePtr[T any] interface {
	*T
}

type snapshotStore[T any] interface {
	Save(ctx context.Context, a *snapshot.Aggregate[T]) error
	Load(ctx context.Context, id identity.ID) *snapshot.Snapshot[T]
}

func (ag *Aggregate[T, PT]) Drain() error {
	close(ag.snapChan)
	if err := ag.es.Drain(); err != nil {
		return fmt.Errorf("aggregate drain failed: %w", err)
	}
	return nil
}

func (ag *Aggregate[T, PT]) startSnapshoting(ctx context.Context) {
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-ag.snapChan:
				if !ok {
					return
				}
				ctxSnap, cancel := context.WithTimeout(ctx, ag.storeConfig.SnapshotTimeout)

				if err := ag.ss.Save(ctxSnap, a); err != nil {
					ag.logger().Error("snapshot save", "error", err.Error())
					cancel()
					continue
				}
				cancel()
				ag.logger().Info("snapshot saved", "aggregateID", a.ID.String(), "version", a.Version, "sequence", a.Sequence)
			}
		}
	}(ctx)
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[State any, PT AggregatePtr[State]](ctx context.Context, es stream.Store, ss snapshot.Store, opts ...Option[State]) (*Aggregate[State, PT], error) {
	opt := new(storeOptions[State])
	opt.storeConfig = storeConfig{
		SnapshotMsgThreshold: snapshot.DefaultSizeInEvents,
		SnapshotMinInterval:  snapshot.DefaultMinInterval,
		SnapshotTimeout:      snapshot.DefaultTimeout,
		Logger:               prettylog.NewDefault(),
		SnapshotMaxTasks:     255,
	}
	for _, o := range opts {
		if err := o(opt); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}

	}
	str, err := stream.New(es, opt.streamOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create event stream: %w", err)
	}
	snap := snapshot.NewStore[State](ss, opt.snapshotOptions...)

	aggr := &Aggregate[State, PT]{
		storeConfig: opt.storeConfig,
		es:          str,
		ss:          snap,
		snapChan:    make(chan *snapshot.Aggregate[State], opt.SnapshotMaxTasks),
		eventTypes:  str,
	}
	aggr.startSnapshoting(ctx)
	//	var zero T

	return aggr, nil
}

// Subscriber is an interface that defines the Project method for projecting events on an
// Events must implement the Event interface.
type Subscriber[Aggregate any] interface {
	Subscribe(ctx context.Context, h EventsHandler[Aggregate], opts ...stream.ProjOption) error
}
type eventKindSubscriber[Aggregate any] interface {
	Subscriber[Aggregate]
	EventKind(in Evolver[Aggregate]) (string, error)
}

type CommandHandler[Aggregate any, Command any] interface {
	HandleCommand(ctx context.Context, cmd Command) ([]stream.EventMetadata, error)
}

type EventsHandler[Aggregate any] interface {
	HandleEvents(ctx context.Context, event Evolver[Aggregate]) error
}

type EventHandler[Aggregate any, Event Evolver[Aggregate]] interface {
	HandleEvent(ctx context.Context, event Event) error
}

type eventStream interface {
	LoadEvents(ctx context.Context, id identity.ID, seq uint64) iter.Seq2[*stream.Event, error]
	SaveEvents(ctx context.Context, id identity.ID, expectedSequence uint64, events []any) ([]stream.EventMetadata, error)
	Subscribe(ctx context.Context, h stream.EventHandler, opts ...stream.ProjOption) error
	Drain() error
}

// Mutator is an interface that defines the Update method for executing commands on an
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Executer interface.
type Mutator[Aggregate any, PT AggregatePtr[Aggregate]] interface {
	Mutate(ctx context.Context, id ID, modify func(state PT) (Events[Aggregate], error)) ([]stream.EventMetadata, error)
}

type eventKinder interface {
	EventKind(in any) (string, error)
}

func (a *Aggregate[T, PT]) logger() logger {
	return a.storeConfig.Logger
}

type Aggregate[T any, PT AggregatePtr[T]] struct {
	storeConfig storeConfig
	es          eventStream
	ss          snapshotStore[T]
	eventTypes  eventKinder
	snapChan    chan *snapshot.Aggregate[T]
}

func (a *Aggregate[T, PT]) EventKind(in Evolver[T]) (string, error) {
	return a.eventTypes.EventKind(in)
}

func (a *Aggregate[T, PT]) build(ctx context.Context, id ID, sn *snapshot.Snapshot[T]) (*snapshot.Aggregate[T], error) {
	aggr := &snapshot.Aggregate[T]{
		ID:    identity.ID(id),
		State: PT(new(T)),
	}

	if sn != nil {
		aggr = sn.Body
	}

	events := a.es.LoadEvents(ctx, identity.ID(id), aggr.Sequence)
	for event, err := range events {
		if err != nil {
			if errors.Is(err, stream.ErrNoEvents) {
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
			aggr.Timestamp = event.Timestamp

		} else {
			return nil, fmt.Errorf("buid, event is not Evolver[T] %w", err)
		}

	}

	return aggr, nil
}

func (a *Aggregate[T, PT]) Subscribe(ctx context.Context, h EventsHandler[T], opts ...stream.ProjOption) error {
	var op []stream.ProjOption

	op = append(op, stream.WithName(typeregistry.CreateNameFromType(h)))
	op = append(op, opts...)
	if err := a.es.Subscribe(ctx, &subscribeHandlerAdapter[T]{h: h}, op...); err != nil {
		return fmt.Errorf("subscription failed: %w", err)
	}

	return nil
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

			select {
			case a.snapChan <- aggr:

			default:
				a.logger().Error("snapshot save", "error", "snapshot queue is full")
			}

		}
	}

	return storedMsgs, invError
}

type handleEventAdapter[E Evolver[T], T any] struct {
	h EventHandler[T, E]
}

func (h *handleEventAdapter[E, T]) HandleEvents(ctx context.Context, event Evolver[T]) error {
	if e, ok := event.(E); ok {
		return h.h.HandleEvent(ctx, e)
	}
	return stream.NonRetriableError{Err: fmt.Errorf("event is not Evolver[T]")}
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

type pointerEvent[E any, T any] interface {
	*E
	Evolver[T]
}

func ProjectEvent[E any, T any, PE pointerEvent[E, T]](ctx context.Context, sub eventKindSubscriber[T], h EventHandler[T, PE]) error {
	var sentinel PE

	n := fmt.Sprintf("%s", typeregistry.CreateNameFromType(h))

	eventKind, err := sub.EventKind(sentinel)
	if err != nil {
		return fmt.Errorf("project event failed: %w", err)
	}
	if err := sub.Subscribe(
		ctx,
		&handleEventAdapter[PE, T]{h: h},
		stream.WithFilterByEvent(eventKind),
		stream.WithName(n),
	); err != nil {
		return fmt.Errorf("project event failed: %w", err)
	}
	return nil

}
