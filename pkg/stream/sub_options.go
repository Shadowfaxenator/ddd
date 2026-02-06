package stream

import (
	"github.com/alekseev-bro/ddd/pkg/qos"
)

type ProjOption func(p *SubscribeParams)
type SubscribeParams struct {
	DurableName string
	Kind        []string
	AggrID      string
	QoS         qos.QoS
	Reg         nameForer
}

func WithFilterByAggregateID(id string) ProjOption {
	return func(p *SubscribeParams) {
		p.AggrID = id
	}
}

func WithFilterByEvent(kind string) ProjOption {

	return func(p *SubscribeParams) {
		p.Kind = append(p.Kind, kind)
	}
}

func WithName(name string) ProjOption {
	return func(p *SubscribeParams) {
		p.DurableName = name
	}
}

func WithQoS(qos qos.QoS) ProjOption {
	return func(p *SubscribeParams) {
		p.QoS = qos
	}
}
