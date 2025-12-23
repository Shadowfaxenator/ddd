package aggregate

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"fmt"

	"github.com/alekseev-bro/ddd/pkg/qos"
	"github.com/alekseev-bro/ddd/pkg/store"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/internal/typereg"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// var nc *nats.Conn

type messageCount uint

const (
	snapshotSize      messageCount  = 100
	snapshotInterval  time.Duration = time.Second * 1
	idempotancyWindow time.Duration = time.Minute * 2
)

type InvariantViolationError struct {
	Err error
}

func (e InvariantViolationError) Error() string {
	return e.Err.Error()
}

// AggregateNameFromType returns the aggregate name and bounded context name from the type T.
func AggregateNameFromType[T Aggregatable]() (aname string, bctx string) {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		panic("T must be a struct")
	}

	aname = t.Name()
	sep := strings.Split(t.PkgPath(), "/")
	bctx = sep[len(sep)-1]
	return
}

type Serder serde.Serder

func (*Root[T]) NewID() ID[T] {
	a, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return ID[T](a.String())
}

// SetTypeEncoder sets the default encoder for the aggregate,
// encoder must be a Serder implementation.
// Default is JSON.
func SetTypeEncoder(s Serder) {
	serde.SetDefaultSerder(s)
}

type snapshotThreshold struct {
	numMsgs  byte
	interval time.Duration
}

// New creates a new aggregate root using the provided event stream and snapshot store.
func New[T Aggregatable](ctx context.Context, es EventStream[T], ss SnapshotStore[T], opts ...Option[T]) *Root[T] {

	aggr := &Root[T]{
		snapshotThreshold: snapshotThreshold{interval: snapshotInterval, numMsgs: byte(snapshotSize)},
		es:                es,
		ss:                ss,
	}
	for _, o := range opts {
		o(aggr)
	}
	//	var zero T

	return aggr
}

type Envelope struct {
	ID            uuid.UUID
	Version       uint64
	AggregateID   string
	AggregateKind string
	Kind          string
	Payload       []byte
}

// Executer is an interface that defines the Execute method for executing commands on an aggregate.
// Each command is executed in a transactional manner, ensuring that the aggregate state is consistent.
// Commands must implement the Command interface.
type Executer[T Aggregatable] interface {
	Execute(ctx context.Context, idempotencyKey string, command Command[T]) (CommandID[T], error)
}

// Projector is an interface that defines the Project method for projecting events on an aggregate.
// Events must implement the Event interface.
type Projector[T Aggregatable] interface {
	Project(ctx context.Context, h EventHandler[T], opts ...ProjOption) (Drainer, error)
}

// All aggregates must implement the Aggregate interface.
type Aggregate[T Aggregatable] interface {
	Projector[T]
	Executer[T]
	NewID() ID[T]
}

// Use to drain all open connections
type Drainer interface {
	Drain() error
}

type subscriber interface {
	Subscribe(ctx context.Context, handler func(envel *Envelope) error, params *SubscribeParams) (Drainer, error)
}

type ProjOption func(p *SubscribeParams)

type EventStream[T Aggregatable] interface {
	Save(ctx context.Context, aggrID string, idempotencyKey string, event *Envelope) error
	Load(ctx context.Context, aggrID string, fromSeq uint64) ([]*Envelope, error)
	subscriber
}

type SnapshotStore[T Aggregatable] interface {
	Save(ctx context.Context, aggrID string, snap []byte) error
	Load(ctx context.Context, aggrID string) ([]byte, error)
}

// Aggregate root type it implements the Aggregate interface.
type Root[T Aggregatable] struct {
	snapshotThreshold snapshotThreshold
	es                EventStream[T]
	ss                SnapshotStore[T]
	qos               qos.QoS
	pubsub            *nats.Conn
}

type snapshot[T Aggregatable] struct {
	old       bool
	Timestamp time.Time
	MsgCount  messageCount
	Version   uint64
	Body      *T
}

func (a *Root[T]) build(ctx context.Context, id ID[T]) (*snapshot[T], error) {

	var snap snapshot[T]

	rec, err := a.ss.Load(ctx, id.String())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNoSnapshot):
			snap.Body = new(T)
		default:
			return nil, fmt.Errorf("build: %w", err)
		}
	} else {
		if err := serde.Deserialize(rec, &snap); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
	}

	envelopes, err := a.es.Load(ctx, id.String(), snap.Version)
	if err != nil {
		if !errors.Is(err, store.ErrNoAggregate) {
			return nil, fmt.Errorf("buid %w", err)
		}

	}
	if envelopes != nil {
		for _, e := range envelopes {
			ev := typereg.GetType(e.Kind, e.Payload)

			ev.(Event[T]).Apply(snap.Body)

		}

		snap.Version = envelopes[len(envelopes)-1].Version
		snap.MsgCount = snap.MsgCount + messageCount(len(envelopes))
		if messageCount(len(envelopes)) >= messageCount(a.snapshotThreshold.numMsgs) && time.Since(snap.Timestamp) > a.snapshotThreshold.interval {
			snap.old = true
		}
	}

	if snap.MsgCount == 0 {
		snap.Body = nil
	}

	return &snap, nil
}

// Execute executes a command on the aggregate root.
func (a *Root[T]) Execute(ctx context.Context, idempKey string, command Command[T]) (CommandID[T], error) {

	commandUUID := CommandID[T](uuid.NewSHA1(command.AggregateID().UUID(), []byte(idempKey)).String())

	var err error

	snap, err := a.build(ctx, command.AggregateID())
	if err != nil {
		return commandUUID, fmt.Errorf("build aggrigate: %w", err)
	}

	evt, err := command.Execute(snap.Body)

	if err != nil {
		return commandUUID, &InvariantViolationError{Err: err}
	}

	b, err := serde.Serialize(evt)
	if err != nil {
		slog.Error("command serialize", "error", err)
		panic(err)
	}

	// Panics if event isn't registered
	kind := typereg.GuardType(evt)

	if err := a.es.Save(ctx, command.AggregateID().String(), commandUUID.String(), &Envelope{Version: snap.Version, Payload: b, Kind: kind}); err != nil {
		return commandUUID, fmt.Errorf("command: %w", err)
	}

	// Save snapshot if aggregate has more than snapshotThreshold messages
	if snap.old {
		go func() {
			snap.Timestamp = time.Now()
			b, err := serde.Serialize(snap)
			if err != nil {
				slog.Warn("snapshot save serialization", "error", err.Error())
				return
			}
			err = a.ss.Save(ctx, command.AggregateID().String(), b)
			if err != nil {
				slog.Error("snapshot save", "error", err.Error())
				return
			}
			slog.Info("snapshot saved", "version", snap.Version, "aggregateID", command.AggregateID(), "msg_count", snap.MsgCount, "aggregate", reflect.TypeFor[T]().Name())

		}()

	}

	return commandUUID, nil
}
