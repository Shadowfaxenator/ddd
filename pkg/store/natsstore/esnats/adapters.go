package esnats

import (
	"log/slog"
	"strings"

	"github.com/alekseev-bro/ddd/internal/typereg"
	"github.com/alekseev-bro/ddd/pkg/eventstore"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsMessage interface {
	Headers() nats.Header
	Data() []byte
	Subject() string
	Seq() uint64
}

type jsRawMsgAdapter struct {
	*jetstream.RawStreamMsg
}

func (j jsRawMsgAdapter) Headers() nats.Header {
	return j.RawStreamMsg.Header
}

func (j jsRawMsgAdapter) Data() []byte {
	return j.RawStreamMsg.Data
}

func (j jsRawMsgAdapter) Subject() string {
	return j.RawStreamMsg.Subject
}

func (j jsRawMsgAdapter) Seq() uint64 {
	return j.RawStreamMsg.Sequence
}

type natsMessageAdapter struct {
	*nats.Msg
}

func (n natsMessageAdapter) Headers() nats.Header {
	return n.Msg.Header
}

func (n natsMessageAdapter) Data() []byte {
	return n.Msg.Data
}

func (n natsMessageAdapter) Subject() string {
	return n.Msg.Subject
}

// TODO: check panic for NATS core
func (n natsMessageAdapter) Seq() uint64 {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Sequence.Stream
}

type natsJSMsgAdapter struct {
	jetstream.Msg
}

func (n natsJSMsgAdapter) Seq() uint64 {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Sequence.Stream
}

func eventFromMsg[T any](msg natsMessage) *eventstore.Event[T] {

	uuid, err := uuid.Parse(msg.Headers().Get(jetstream.MsgIDHeader))
	if err != nil {
		slog.Error("failed to parse uuid", "error", err)
		panic("failed to parse uuid")
	}
	subjectParts := strings.Split(msg.Subject(), ".")
	kind := subjectParts[2]

	ev := typereg.GetType(kind, msg.Data())
	return &eventstore.Event[T]{
		ID:      eventstore.EventID[T](uuid),
		Kind:    kind,
		Version: msg.Seq(),
		Body:    ev.(eventstore.Applyer[T]),
	}
}
