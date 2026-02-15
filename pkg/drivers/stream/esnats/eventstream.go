package esnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"
	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/alekseev-bro/ddd/pkg/qos"
	"github.com/alekseev-bro/ddd/pkg/stream"

	"math"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

const (
	maxAckPending int = 1000
	eventIDHeader     = "X-Event-ID"
)

type StoreType jetstream.StorageType

const (
	Disk StoreType = iota
	Memory
)

type eventStreamDriver struct {
	name string
	EventStreamConfig
	// TODO: impl partitioning
	js jetstream.JetStream
}

const (
	defaultDeduplication time.Duration = time.Minute * 2
)

func NewDriver(ctx context.Context, js jetstream.JetStream, name string, cfg EventStreamConfig) (*eventStreamDriver, error) {
	if cfg.Deduplication == 0 {
		cfg.Deduplication = defaultDeduplication
	}
	stream := &eventStreamDriver{name: name, js: js, EventStreamConfig: cfg}

	_, err := stream.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Subjects:           []string{stream.allSubjects()},
		Name:               name,
		Storage:            jetstream.StorageType(cfg.StoreType),
		Duplicates:         cfg.Deduplication,
		AllowDirect:        true,
		AllowAtomicPublish: true,
	})

	if err != nil {
		return nil, fmt.Errorf("nats stream create: %w", err)
	}

	return stream, nil
}

func (s *eventStreamDriver) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s.%s", s.name, agrid)
}
func (s *eventStreamDriver) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s.%s.>", s.name, agrid)
}

func (s *eventStreamDriver) allSubjects() string {
	return fmt.Sprintf("%s.>", s.name)
}

func (s *eventStreamDriver) Save(ctx context.Context, aggrID int64, expectedSequence uint64, msgs []stream.Msg, idempotancyKey int64) ([]stream.MsgMetadata, error) {
	strAggrID := strconv.FormatInt(aggrID, 10)
	if msgs == nil {
		return nil, nil
	}

	nmsgs := make([]*nats.Msg, len(msgs))
	for i, msg := range msgs {
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msg.Kind)
		nmsg := nats.NewMsg(sub)

		if idempotancyKey != 0 && i == 0 {
			nmsg.Header.Add(jetstream.MsgIDHeader, strconv.FormatInt(idempotancyKey, 10))
		}
		nmsg.Data = msg.Body
		nmsg.Header.Add(eventIDHeader, strconv.FormatInt(msg.ID, 10))

		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqSubjHeader, s.allSubjectsForID(strAggrID))
		nmsg.Header.Add(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(expectedSequence, 10))
		nmsgs[i] = nmsg
	}

	if len(nmsgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, nmsgs[0])
		if err != nil {
			var seqerr *jetstream.APIError
			if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				slog.Warn("occ", "version", expectedSequence, "name", s.subjectNameForID(strAggrID))
			}
			return nil, fmt.Errorf("store event func: %w", err)
		}
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msgs[0].Kind)
		if ack.Duplicate {
			slog.Warn("duplicate event not stored", "ID", msgs[0].ID, "kind", msgs[0].Kind, "subject", sub, "stream", s.name)
			return nil, nil
		}

		slog.Info("event stored", "ID", msgs[0].ID, "kind", msgs[0].Kind, "subject", sub, "stream", s.name)
		msgs := []stream.MsgMetadata{
			{
				MsgID:    msgs[0].ID,
				Sequence: ack.Sequence,
			},
		}
		return msgs, nil
	}

	batchAck, err := jetstreamext.PublishMsgBatch(ctx, s.js, nmsgs, jetstreamext.BatchFlowControl{AckEvery: 1, AckTimeout: time.Second})
	if err != nil {
		var seqerr *jetstream.APIError
		if errors.As(err, &seqerr); seqerr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			slog.Warn("occ", "name", s.subjectNameForID(strAggrID))
		}
		return nil, fmt.Errorf("save: %w", err)
	}

	outmsgs := make([]stream.MsgMetadata, len(msgs))
	for i, msg := range msgs {
		outmsgs[i] = stream.MsgMetadata{
			MsgID:    msg.ID,
			Sequence: batchAck.Sequence + uint64(i),
		}
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msg.Kind)
		slog.Info("event stored", "ID", msg.ID, "kind", msg.Kind, "subject", sub, "stream", s.name)
	}

	return outmsgs, nil
}

func (s *eventStreamDriver) Load(ctx context.Context, aggrID int64, fromSeq uint64, handler func(*stream.StoredMsg) error) error {
	strAggrID := strconv.FormatInt(aggrID, 10)
	subj := s.allSubjectsForID(strAggrID)
	msgs, err := jetstreamext.GetBatch(ctx,
		s.js, s.name, math.MaxInt, jetstreamext.GetBatchSubject(subj),
		jetstreamext.GetBatchSeq(fromSeq+1))
	//fmt.Println(time.Since(start))

	if err != nil {
		return fmt.Errorf("get events: %w", err)
	}

	for msg, err := range msgs {

		if err != nil {
			if errors.Is(err, jetstreamext.ErrNoMessages) {

				return eventstore.ErrNoAggregate
			}
			return fmt.Errorf("build func can't get msg batch: %w", err)
		}
		sm, err := streamMsgFromNatsMsg(jsRawMsgAdapter{msg})
		if err != nil {
			return fmt.Errorf("load failed: %w", err)
		}
		if err := handler(sm); err != nil {
			return err
		}

	}

	return nil
}

type drainAdapter struct {
	jetstream.ConsumeContext
}

func (d *drainAdapter) Drain() error {
	d.ConsumeContext.Drain()
	return nil
}

type drainList []stream.Drainer

func (d drainList) Drain() error {
	for _, drainer := range d {
		if err := drainer.Drain(); err != nil {
			return err
		}
	}
	return nil
}

func aggrIDFromParams(params *stream.SubscribeParams) string {
	if params.AggrID != "" {
		return params.AggrID
	}

	return "*"
}

func (e *eventStreamDriver) Subscribe(ctx context.Context, handler func(msg *stream.StoredMsg) error, params *stream.SubscribeParams) (stream.Drainer, error) {

	maxpend := maxAckPending
	if params.QoS.Ordering == qos.Ordered {
		maxpend = 1
	}

	var filter []string
	if params.Kind != nil {
		for _, kind := range params.Kind {

			filter = append(filter, fmt.Sprintf("%s.%s.%s", e.name, aggrIDFromParams(params), kind))
		}
	} else {
		filter = append(filter, fmt.Sprintf("%s.%s.%s", e.name, aggrIDFromParams(params), "*"))
	}
	if params.QoS.Delivery == qos.AtMostOnce {
		subs := make(drainList, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {

				var target *aggregate.InvariantViolationError
				sm, err := streamMsgFromNatsMsg(natsMessageAdapter{msg})
				if err != nil {
					slog.Error("load failed", "error", err)
					return
				}
				if err := handler(sm); err != nil {
					if !errors.As(err, &target) {
						slog.Warn("redelivering", "error", err)
						msg.Nak()
						return
					} else {
						slog.Warn("invariant violation", "reason", err.Error())
					}
				}
				msg.Ack()

			})
			if err != nil {
				return nil, fmt.Errorf("at most once subscribe: %w", err)
			}
			subs[i] = sub
		}

		return subs, nil
	}

	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.name, jetstream.ConsumerConfig{
		Durable:        params.DurableName,
		FilterSubjects: filter,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  maxpend,
		// PriorityPolicy: jetstream.PriorityPolicyPinned,
		// PriorityGroups: []string{"sub"},
		//PinnedTTL:      time.Second * 10,
	})
	if err != nil {
		return nil, fmt.Errorf("subscription create consumer: %w", err)
	}

	ct, err := cons.Consume(func(msg jetstream.Msg) {

		var target *aggregate.InvariantViolationError
		sm, err := streamMsgFromNatsMsg(natsJSMsgAdapter{msg})
		if err != nil {
			slog.Error("load failed", "error", err)
			return
		}
		if err := handler(sm); err != nil {
			if !errors.As(err, &target) {
				slog.Warn("redelivering", "error", err)
				msg.Nak()
				return
			} else {
				slog.Warn("invariant violation", "reason", err.Error())
			}
		}
		msg.Ack()

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		slog.Error("subscription consume", "error", err)
	}),
	)
	if err != nil {
		return nil, fmt.Errorf("subscription consume: %w", err)
	}

	return drainList{&drainAdapter{ConsumeContext: ct}}, nil

}
