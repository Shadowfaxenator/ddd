package esnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/alekseev-bro/ddd/internal/serde"
	"github.com/alekseev-bro/ddd/pkg/qos"
	"github.com/alekseev-bro/ddd/pkg/store"

	"github.com/alekseev-bro/ddd/pkg/eventstore"

	"math"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

const (
	maxAckPending int = 1000
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type eventStream[T any] struct {
	dedupe    time.Duration
	storeType StoreType
	// TODO: impl partitioning
	partnum    uint8
	tname      string
	boundedCtx string
	js         jetstream.JetStream
}

func NewEventStream[T any](ctx context.Context, js jetstream.JetStream, opts ...Option[T]) *eventStream[T] {
	aname, bcname := eventstore.AggregateNameFromType[T]()

	stream := &eventStream[T]{js: js, tname: aname, boundedCtx: bcname}

	for _, opt := range opts {
		opt(stream)
	}

	_, err := stream.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Subjects:           []string{stream.allSubjects()},
		Name:               stream.streamName(),
		Storage:            jetstream.StorageType(stream.storeType),
		Duplicates:         stream.dedupe,
		AllowDirect:        true,
		AllowAtomicPublish: true,
	})

	if err != nil {
		panic(err)
	}

	return stream
}

func (s *eventStream[T]) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s:%s.%s", s.boundedCtx, s.tname, agrid)
}
func (s *eventStream[T]) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s:%s.%s.>", s.boundedCtx, s.tname, agrid)
}

func (s *eventStream[T]) streamName() string {
	return fmt.Sprintf("%s:%s", s.boundedCtx, s.tname)
}

func (s *eventStream[T]) allSubjects() string {
	return fmt.Sprintf("%s.>", s.streamName())
}

func (s *eventStream[T]) Save(ctx context.Context, aggrID string, msgs []*eventstore.Event[T]) error {

	if msgs == nil {
		return nil
	}
	nmsgs := make([]*nats.Msg, len(msgs))
	for i, msg := range msgs {
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(aggrID), msg.Kind)
		nmsg := nats.NewMsg(sub)
		b, err := serde.Serialize(msg.Body)
		if err != nil {
			slog.Error("command serialize", "error", err)
			panic(err)
		}
		nmsg.Data = b
		nmsg.Header.Add(jetstream.MsgIDHeader, msg.ID.String())
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqSubjHeader, s.allSubjectsForID(aggrID))
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqHeader, strconv.Itoa(int(msg.PrevVersion)))
		nmsgs[i] = nmsg
	}

	if len(nmsgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, nmsgs[0])
		if err != nil {
			var seqerr *jetstream.APIError
			if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				slog.Warn("occ", "version", msgs[0].PrevVersion, "name", s.subjectNameForID(aggrID))
			}
			return fmt.Errorf("store event func: %w", err)
		}
		if ack.Duplicate {
			slog.Warn("duplicate event not stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID), "stream", s.streamName())
			return nil
		}
		slog.Info("event stored", "kind", msgs[0].Kind, "subject", s.subjectNameForID(aggrID), "stream", s.streamName())
		return nil
	}

	_, err := jetstreamext.PublishMsgBatch(ctx, s.js, nmsgs, jetstreamext.BatchFlowControl{AckEvery: 1, AckTimeout: time.Second})
	if err != nil {
		var seqerr *jetstream.APIError
		if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			slog.Warn("occ", "name", s.subjectNameForID(aggrID))
		}
		return fmt.Errorf("save: %w", err)
	}

	for _, msg := range msgs {
		slog.Info("event stored", "kind", msg.Kind, "subject", s.subjectNameForID(aggrID), "stream", s.streamName())
	}
	return nil
}

func (s *eventStream[T]) Load(ctx context.Context, id string, version uint64) ([]*eventstore.Event[T], error) {

	subj := s.allSubjectsForID(id)
	msgs, err := jetstreamext.GetBatch(ctx,
		s.js, s.streamName(), math.MaxInt, jetstreamext.GetBatchSubject(subj),
		jetstreamext.GetBatchSeq(version+1))
	//fmt.Println(time.Since(start))

	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	var events []*eventstore.Event[T]
	var prevEv *eventstore.Event[T]
	for msg, err := range msgs {

		if err != nil {
			if errors.Is(err, jetstreamext.ErrNoMessages) {

				return nil, store.ErrNoAggregate
			}
			return nil, fmt.Errorf("build func can't get msg batch: %w", err)
		}

		event := eventFromMsg[T](jsRawMsgAdapter{msg})
		if prevEv == nil {
			event.PrevVersion = version
		} else {
			event.PrevVersion = prevEv.Version
		}

		prevEv = event
		events = append(events, event)
	}
	return events, nil
}

type drainAdapter struct {
	jetstream.ConsumeContext
}

func (d *drainAdapter) Drain() error {
	d.ConsumeContext.Drain()
	return nil
}

type drainList []eventstore.Drainer

func (d drainList) Drain() error {
	for _, drainer := range d {
		if err := drainer.Drain(); err != nil {
			return err
		}
	}
	return nil
}

func aggrIDFromParams(params *eventstore.SubscribeParams) string {
	if params.AggrID != "" {
		return params.AggrID
	}

	return "*"
}

func (e *eventStream[T]) Subscribe(ctx context.Context, handler func(event *eventstore.Event[T]) error, params *eventstore.SubscribeParams) (eventstore.Drainer, error) {

	maxpend := maxAckPending
	if params.QoS.Ordering == qos.Ordered {
		maxpend = 1
	}

	var filter []string
	if params.Kind != nil {
		for _, kind := range params.Kind {
			filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), aggrIDFromParams(params), kind))
		}
	} else {
		filter = append(filter, fmt.Sprintf("%s.%s.%s", e.streamName(), aggrIDFromParams(params), "*"))
	}
	if params.QoS.Delivery == qos.AtMostOnce {
		subs := make(drainList, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {

				handler(eventFromMsg[T](natsMessageAdapter{msg}))
			})
			if err != nil {
				return nil, fmt.Errorf("at most once subscribe: %w", err)
			}
			subs[i] = sub
		}

		return subs, nil
	}
	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.streamName(), jetstream.ConsumerConfig{
		Durable:        params.DurableName,
		FilterSubjects: filter,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  maxpend,
	})
	if err != nil {
		slog.Error("subscription create consumer", "error", err)
		panic(err)
	}
	ct, err := cons.Consume(func(msg jetstream.Msg) {
		// mt, err := msg.Metadata()
		// if err != nil {
		// 	slog.Error("subscription metadata", "error", err)
		// 	slog.Warn("redelivering")
		// 	msg.Nak()
		// 	return
		// }

		var target *eventstore.InvariantViolationError
		if err := handler(eventFromMsg[T](natsJSMsgAdapter{msg})); err != nil {
			if !errors.As(err, &target) {
				slog.Warn("redelivering", "error", err)
				msg.Nak()
				return
			} else {
				slog.Warn("invariant violation", "reason", err.Error())
			}
		}

		msg.Ack()

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {}))
	if err != nil {
		panic(fmt.Errorf("subscription consume: %w", err))
	}
	return drainList{&drainAdapter{ConsumeContext: ct}}, nil

}
