package roquesrv

import (
	"context"
	"fmt"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque/roquemsg"
)

func NewDispatcher() *Dispatcher {
	return &Dispatcher{}
}

type Dispatcher struct {
}

func (d *Dispatcher) WriteContext(ctx context.Context, msg roquemsg.Message) error {
	if msg.Topic == "test.error" {
		return fmt.Errorf("received on test.error")
	}
	log.Infof("dispatcher: write [%s]: %s", msg.Topic, string(msg.Data))
	return nil
}

func (d *Dispatcher) ReadContext(ctx context.Context, clientID string, topic roquemsg.Topic) (roquemsg.Message, error) {
	if topic == "test.error" {
		return roquemsg.Message{}, fmt.Errorf("received on test.error")
	}
	<-time.After(500 * time.Millisecond)

	return roquemsg.Message{
		Topic: topic,
		Data:  []byte(fmt.Sprintf("for [%s] on [%s]: %s", clientID, topic, time.Now().Format(time.RFC3339))),
	}, nil
}
