package server

import (
	"context"
	"fmt"

	"github.com/mazzegi/roque/message"
)

func NewDispatcher(store Store) *Dispatcher {
	return &Dispatcher{
		store: store,
	}
}

type Dispatcher struct {
	store Store
}

func (d *Dispatcher) WriteContext(ctx context.Context, msgs ...message.Message) error {
	err := d.store.Append(msgs...)
	if err != nil {
		return fmt.Errorf("store.append: %w", err)
	}
	return nil
}

func (d *Dispatcher) ReadContext(ctx context.Context, clientID string, topic message.Topic, limit int) ([]message.Message, error) {
	msgs, err := d.store.FetchNext(clientID, string(topic), limit)
	if err != nil {
		return nil, fmt.Errorf("store.fetchnext: %w", err)
	}
	return msgs, nil
}

func (d *Dispatcher) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	err := d.store.Commit(clientID, string(topic), idx)
	if err != nil {
		return fmt.Errorf("store.commit: %w", err)
	}
	return nil
}
