package server

import (
	"context"
	"fmt"

	"github.com/mazzegi/log"
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
	log.Infof("dispatcher: write topic=%q: %s", msg.Topic, string(msg.Data))
	err := d.store.Append(string(msg.Topic), msg.Data)
	if err != nil {
		return fmt.Errorf("store.append: %w", err)
	}
	return nil
}

func (d *Dispatcher) ReadContext(ctx context.Context, clientID string, topic message.Topic) (message.Message, error) {
	data, idx, err := d.store.FetchNext(clientID, string(topic))
	if err != nil {
		log.Debugf("fetchnext: %v", err)
		return message.Message{}, fmt.Errorf("store.fetchnext: %w", err)
	}
	return message.Message{
		Topic: topic,
		Index: idx,
		Data:  data,
	}, nil
}

func (d *Dispatcher) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	log.Infof("dispatcher: commit clientid=%q, topic=%q: %d", clientID, topic, idx)
	err := d.store.Commit(clientID, string(topic), idx)
	if err != nil {
		log.Debugf("commit: %v", err)
		return fmt.Errorf("store.commit: %w", err)
	}
	return nil
}
