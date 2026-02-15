package esnats

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alekseev-bro/ddd/pkg/stream"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ComparableStringer interface {
	fmt.Stringer
	comparable
}
type natsMessage interface {
	Headers() nats.Header
	Data() []byte
	Subject() string
	Seq() uint64
	Timestamp() time.Time
}

type ackNaker interface {
	Nak() error
	Ack() error
}

type jsRawMsgAdapter struct {
	*jetstream.RawStreamMsg
}

func (j jsRawMsgAdapter) Headers() nats.Header {
	return j.RawStreamMsg.Header
}
func (j jsRawMsgAdapter) Timestamp() time.Time {
	return j.RawStreamMsg.Time
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

func newNatsMessageAdapter(msg *nats.Msg) (natsMessageAdapter, error) {
	mt, err := msg.Metadata()
	if err != nil {
		return natsMessageAdapter{}, fmt.Errorf("metadata: %w", err)
	}
	return natsMessageAdapter{
		msg: msg,
		mt:  mt,
	}, nil
}

type natsMessageAdapter struct {
	msg *nats.Msg
	mt  *nats.MsgMetadata
}

func (n natsMessageAdapter) Ack() error {
	return n.msg.Ack()
}

func (n natsMessageAdapter) Nak() error {
	return n.msg.Nak()
}

func (n natsMessageAdapter) Headers() nats.Header {
	return n.msg.Header
}

func (n natsMessageAdapter) Timestamp() time.Time {
	return n.mt.Timestamp
}

func (n natsMessageAdapter) Data() []byte {
	return n.msg.Data
}

func (n natsMessageAdapter) Subject() string {

	return n.msg.Subject
}

func (n natsMessageAdapter) Seq() uint64 {

	return n.mt.Sequence.Stream
}

func newNatsJSMsgAdapter(msg jetstream.Msg) (natsJSMsgAdapter, error) {
	mt, err := msg.Metadata()
	if err != nil {
		return natsJSMsgAdapter{}, fmt.Errorf("metadata: %w", err)
	}

	return natsJSMsgAdapter{
		msg: msg,
		mt:  mt,
	}, nil
}

type natsJSMsgAdapter struct {
	msg jetstream.Msg
	mt  *jetstream.MsgMetadata
}

func (n natsJSMsgAdapter) Ack() error {
	return n.msg.Ack()
}

func (n natsJSMsgAdapter) Nak() error {
	return n.msg.Nak()
}

func (n natsJSMsgAdapter) Timestamp() time.Time {

	return n.mt.Timestamp
}

func (n natsJSMsgAdapter) Seq() uint64 {

	return n.mt.Sequence.Stream
}

func (n natsJSMsgAdapter) Data() []byte {
	return n.msg.Data()
}

func (n natsJSMsgAdapter) Subject() string {

	return n.msg.Subject()
}

func (n natsJSMsgAdapter) Headers() nats.Header {
	return n.msg.Headers()
}

func streamMsgFromNatsMsg(msg natsMessage) (*stream.StoredMsg, error) {

	subjectParts := strings.Split(msg.Subject(), ".")
	kind := subjectParts[2]

	//	ev := typereg.GetType(kind, msg.Data())

	id, err := strconv.ParseInt(msg.Headers().Get(eventIDHeader), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse event ID: %w", err)
	}

	return &stream.StoredMsg{
		ID:        id,
		Kind:      kind,
		Body:      msg.Data(),
		Sequence:  msg.Seq(),
		Timestamp: msg.Timestamp(),
	}, nil
}
