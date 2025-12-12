package domain_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alekseev-bro/ddd/pkg/domain"
	"github.com/alekseev-bro/ddd/pkg/store"
)

type MockSnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string][]byte
}

func (s *MockSnapshotStore) Save(ctx context.Context, id string, snap []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[id] = snap
	return nil
}

func (s *MockSnapshotStore) Load(ctx context.Context, id string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if snap, ok := s.snapshots[id]; ok {
		return snap, nil
	}
	return nil, store.ErrNoSnapshot
}

type MockEventStream struct {
	mus  sync.RWMutex
	subs map[string]sub

	mue    sync.RWMutex
	events []*domain.Envelope

	muk  sync.Mutex
	keys map[string]struct{}
}

var ErrOCC = fmt.Errorf("optimistic concurrency control error")

func (m *MockEventStream) removeSub(key string) {
	m.mus.Lock()
	defer m.mus.Unlock()
	delete(m.subs, key)
}

func (m *MockEventStream) Save(ctx context.Context, aggrID string, idempotencyKey string, envel *domain.Envelope) error {
	m.muk.Lock()
	defer m.muk.Unlock()
	if _, ok := m.keys[idempotencyKey]; ok {
		return nil
	}
	m.keys[idempotencyKey] = struct{}{}

	m.mue.Lock()
	if m.events[len(m.events)-1].Version != envel.Version {
		return ErrOCC

	}
	m.events = append(m.events, envel)
	m.mue.Unlock()
	m.mus.RLock()
	defer m.mus.RUnlock()
	for _, sub := range m.subs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sub.ch <- envel:
		}
	}
	return nil
}

func (m *MockEventStream) Load(ctx context.Context, aggrID string, fromSeq uint64) ([]*domain.Envelope, error) {
	m.mue.RLock()
	defer m.mue.RUnlock()
	var envelopes []*domain.Envelope
	for i, event := range m.events {
		if i >= int(fromSeq) {
			envelopes = append(envelopes, event)
		}
	}
	if len(envelopes) == 0 {
		return nil, store.ErrNoAggregate
	}
	return envelopes, nil
}

type sub struct {
	ch     chan *domain.Envelope
	params *domain.SubscribeParams
}

type drainer struct {
	key string
	m   *MockEventStream
}

func (d *drainer) Drain() error {
	d.m.removeSub(d.key)
	return nil
}

func (m *MockEventStream) Subscribe(ctx context.Context, handler func(envel *domain.Envelope) error, params *domain.SubscribeParams) (domain.Drainer, error) {
	m.mus.Lock()
	defer m.mus.Unlock()
	subch := make(chan *domain.Envelope, 50)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-subch:
				handler(event)
			}
		}
	}()
	m.subs[params.DurableName] = sub{
		ch:     subch,
		params: params,
	}
	return &drainer{
		key: params.DurableName,
		m:   m,
	}, nil
}

func NewMockEventStream() *MockEventStream {
	m := &MockEventStream{
		keys: make(map[string]struct{}),
		subs: make(map[string]sub),
	}

	return m
}

func NewMockSnapshotStore() *MockSnapshotStore {
	m := &MockSnapshotStore{
		snapshots: make(map[string][]byte),
	}

	return m
}

type MockUser struct {
	ID   string
	Name string
}

type UserCreated struct {
	User MockUser
}

func (cc *UserCreated) Apply(c *MockUser) {
	*c = cc.User
}

type CreateUser struct {
	MockUser
}

func (c *CreateUser) AggregateID() domain.ID[MockUser] {
	return domain.ID[MockUser](c.ID)
}

func (c *CreateUser) Execute(a *MockUser) domain.Event[MockUser] {
	if a != nil {

		return &domain.EventError[MockUser]{Reason: "customer already exists"}
	}

	return &UserCreated{User: c.MockUser}
}

func TestAggregate(t *testing.T) {
	//	t.Parallel()

	aggr := domain.NewAggregate[MockUser](context.Background(), NewMockEventStream(), NewMockSnapshotStore())
	t.Run("Aggregate", func(t *testing.T) {
		aggr.Execute(context.Background(), "test", &CreateUser{MockUser{Name: "test"}})
	})

}
