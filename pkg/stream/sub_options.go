package stream

import (
	"github.com/alekseev-bro/ddd/pkg/qos"
)

type ProjOption func(p *SubscribeParams)

type SubscribeParams struct {
	DurableName string
	Kind        []string
	AggregateID string
	QoS         qos.QoS
}

func WithFilterByAggregateID(id string) ProjOption {
	return func(p *SubscribeParams) {
		p.AggregateID = id
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
