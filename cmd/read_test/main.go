package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque/client"
	"github.com/mazzegi/roque/message"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	clt, err := client.DialContext(ctx, "127.0.0.1:7001")
	if err != nil {
		panic(err)
	}
	clientID := "test.client"
	topic := "test.topic"

	for {
		msg, err := clt.ReadContext(ctx, clientID, message.Topic(topic))
		if err != nil {
			log.Infof("ended with error: %v", err)
			break
		}
		log.Infof("recv: [%s:%d]: %s", msg.Topic, msg.Index, string(msg.Data))
		clt.CommitContext(ctx, clientID, msg.Topic, msg.Index)
	}

}
