package esnats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/alekseev-bro/ddd/internal/prettylog"
	"github.com/alekseev-bro/ddd/pkg/aggregate"
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

type eventStore struct {
	name string
	*eventStreamConfig
	// TODO: impl partitioning
	js jetstream.JetStream
}

const (
	defaultDeduplication time.Duration = time.Minute * 2
)

func NewStore(ctx context.Context, js jetstream.JetStream, name string, opts ...Option) (*eventStore, error) {
	cfg := &eventStreamConfig{
		StoreType:     Disk,
		Deduplication: defaultDeduplication,
		Logger:        prettylog.NewDefault(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	stream := &eventStore{name: name, js: js, eventStreamConfig: cfg}

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

func (s *eventStore) subjectNameForID(agrid string) string {
	return fmt.Sprintf("%s.%s", s.name, agrid)
}
func (s *eventStore) allSubjectsForID(agrid string) string {
	return fmt.Sprintf("%s.%s.>", s.name, agrid)
}

func (s *eventStore) allSubjects() string {
	return fmt.Sprintf("%s.>", s.name)
}

func (s *eventStore) Save(ctx context.Context, aggrID int64, expectedSequence uint64, msgs []stream.Msg, idempotencyKey int64) ([]stream.EventMetadata, error) {
	strAggrID := strconv.FormatInt(aggrID, 10)
	if msgs == nil {
		return nil, stream.NonRetriableError{Err: errors.New("no messages to save")}
	}

	nmsgs := make([]*nats.Msg, len(msgs))
	for i, msg := range msgs {
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msg.Kind)
		nmsg := nats.NewMsg(sub)

		if idempotencyKey != 0 && i == 0 {
			nmsg.Header.Add(jetstream.MsgIDHeader, strconv.FormatInt(idempotencyKey, 10))
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
				s.Logger.Warn("occ", "version", expectedSequence, "name", s.subjectNameForID(strAggrID))
			}
			return nil, fmt.Errorf("store event func: %w", err)
		}
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msgs[0].Kind)
		if ack.Duplicate {
			s.Logger.Warn("duplicate event not stored", "ID", msgs[0].ID, "kind", msgs[0].Kind, "subject", sub, "stream", s.name)
			return nil, stream.NonRetriableError{Err: errors.New("duplicate event")}
		}

		s.Logger.Info("event stored", "ID", msgs[0].ID, "kind", msgs[0].Kind, "subject", sub, "stream", s.name)
		msgs := []stream.EventMetadata{
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
			s.Logger.Warn("occ", "name", s.subjectNameForID(strAggrID))
		}
		return nil, fmt.Errorf("save: %w", err)
	}
	msglen := len(msgs)
	outmsgs := make([]stream.EventMetadata, msglen)
	for i, msg := range msgs {
		outmsgs[i] = stream.EventMetadata{
			MsgID:    msg.ID,
			Sequence: batchAck.Sequence - uint64(msglen) + uint64(i) + 1,
		}
		sub := fmt.Sprintf("%s.%s", s.subjectNameForID(strAggrID), msg.Kind)
		s.Logger.Info("event stored", "ID", msg.ID, "kind", msg.Kind, "subject", sub, "stream", s.name)
	}

	return outmsgs, nil
}

func (s *eventStore) Load(ctx context.Context, aggrID int64, fromSeq uint64, handler func(*stream.StoredMsg) error) error {
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

				return aggregate.ErrNotExists
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		d.ConsumeContext.Drain()
		done <- nil
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func aggrIDFromParams(params *stream.SubscribeParams) string {
	if params.AggrID != "" {
		return params.AggrID
	}

	return "*"
}

func (e *eventStore) processMessage(m natsMessage, a ackNaker, handler func(msg *stream.StoredMsg) error) {

	sm, err := streamMsgFromNatsMsg(m)
	if err != nil {
		e.Logger.Error("load failed", "error", err)
		return
	}
	if err := handler(sm); err != nil {
		if nonRetErr, ok := errors.AsType[stream.NonRetriableError](err); ok {
			e.Logger.Warn("invariant violation", "reason", nonRetErr.Err.Error())
		} else {
			e.Logger.Warn("redelivering...", "error", err)
			a.Nak()
			return
		}
	}
	a.Ack()
}

func (e *eventStore) Subscribe(ctx context.Context, handler func(msg *stream.StoredMsg) error, params *stream.SubscribeParams) (stream.Drainer, error) {

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
		subs := make(stream.DrainList, len(filter))
		for i, f := range filter {
			sub, err := e.js.Conn().Subscribe(f, func(msg *nats.Msg) {
				adapt := newNatsMessageAdapter(msg)
				e.processMessage(adapt, adapt, handler)
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
		adapt, err := newNatsJSMsgAdapter(msg)
		if err != nil {
			e.Logger.Error("failed to create message adapter", "error", err)
			return
		}
		e.processMessage(adapt, adapt, handler)

	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		e.Logger.Error("subscription consume", "error", err)
	}),
	)
	if err != nil {
		return nil, fmt.Errorf("subscription consume: %w", err)
	}

	return stream.DrainList{&drainAdapter{ConsumeContext: ct}}, nil

}
