package roque

import (
	"context"
	"fmt"
	"time"

	"github.com/mazzegi/roque/message"
)

func NewDispatcher(store Store) *Dispatcher {
	return &Dispatcher{
		store: store,
		sig:   NewSignal(),
	}
}

type Dispatcher struct {
	store Store
	sig   *Signal
}

func (d *Dispatcher) WriteContext(ctx context.Context, topic string, msgs ...[]byte) error {
	err := d.store.Append(topic, msgs...)
	if err != nil {
		return fmt.Errorf("store.append: %w", err)
	}
	d.sig.Emit(topic)
	return nil
}

func (d *Dispatcher) ReadContext(ctx context.Context, clientID string, topic message.Topic, limit int, wait time.Duration) ([]message.Message, error) {
	msgs, err := d.store.FetchNext(clientID, string(topic), limit)
	if err != nil {
		return nil, fmt.Errorf("store.fetchnext: %w", err)
	}
	if len(msgs) > 0 || wait == 0 {
		return msgs, nil
	}
	if !d.sig.WaitContext(ctx, string(topic), wait) {
		return msgs, nil
	}
	// read again without timeout
	return d.ReadContext(ctx, clientID, topic, limit, 0)
}

func (d *Dispatcher) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	err := d.store.Commit(clientID, string(topic), idx)
	if err != nil {
		return fmt.Errorf("store.commit: %w", err)
	}
	return nil
}
