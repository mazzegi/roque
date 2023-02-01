package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque/client"
	"github.com/mazzegi/roque/message"
)

func main() {
	topic := flag.String("topic", "test.topic", "topic to read from")
	clientID := flag.String("client", "test.client", "clinet id")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	clt, err := client.DialContext(ctx, "127.0.0.1:7001")
	if err != nil {
		panic(err)
	}
	// clientID := "test.client"
	// topic := "test.topic"
	log.Infof("reading from %q as %q", *topic, *clientID)
	limit := 5
	var timeout time.Duration = 1 * time.Minute
	for {
		msgs, err := clt.ReadContext(ctx, *clientID, message.Topic(*topic), limit, timeout)
		if err != nil {
			log.Infof("ended with error: %v", err)
			break
		}
		if len(msgs) == 0 {
			log.Infof("no messages")
			break
		}
		for _, msg := range msgs {
			log.Infof("recv: [%s:%d]: %s", msg.Topic, msg.Index, string(msg.Data))
		}
		lastMsg := msgs[len(msgs)-1]
		clt.CommitContext(ctx, *clientID, lastMsg.Topic, lastMsg.Index)
	}
}
