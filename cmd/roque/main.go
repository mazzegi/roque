package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

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
	//writeContext(ctx, clt)
	//readContext(ctx, clt)
	streamContext(ctx, clt)
	//writeContextError(ctx, clt)
}

func writeContext(ctx context.Context, clt *client.Client) {
	err := clt.WriteContext(ctx, message.Message{
		Topic: "test.write",
		Data:  []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339))),
	})
	if err != nil {
		panic(err)
	}
}

func writeContextError(ctx context.Context, clt *client.Client) {
	err := clt.WriteContext(ctx, message.Message{
		Topic: "test.error",
		Data:  []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339))),
	})
	if err != nil {
		panic(err)
	}
}

func readContext(ctx context.Context, clt *client.Client) {
	msg, err := clt.ReadContext(ctx, "test.client", "test.topic")
	if err != nil {
		panic(err)
	}
	log.Infof("recv: [%s]: %s", msg.Topic, string(msg.Data))
}

func streamContext(ctx context.Context, clt *client.Client) {
	msgC, err := clt.StreamContext(ctx, "test.client", "test.topic")
	if err != nil {
		panic(err)
	}
	for msg := range msgC {
		log.Infof("recv: [%s]: %s", msg.Topic, string(msg.Data))
	}
}
