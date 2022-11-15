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

func (d *Dispatcher) WriteContext(ctx context.Context, msg message.Message) error {
	err := d.store.Append(string(msg.Topic), msg.Data)
	if err != nil {
		return fmt.Errorf("store.append: %w", err)
	}
	return nil
}

func (d *Dispatcher) ReadContext(ctx context.Context, clientID string, topic message.Topic) (message.Message, error) {
	data, idx, err := d.store.FetchNext(clientID, string(topic))
	if err != nil {
		return message.Message{}, fmt.Errorf("store.fetchnext: %w", err)
	}
	return message.Message{
		Topic: topic,
		Index: idx,
		Data:  data,
	}, nil
}

func (d *Dispatcher) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	err := d.store.Commit(clientID, string(topic), idx)
	if err != nil {
		return fmt.Errorf("store.commit: %w", err)
	}
	return nil
}
