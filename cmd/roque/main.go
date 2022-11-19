package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque/client"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	clt, err := client.DialContext(ctx, "127.0.0.1:7001")
	if err != nil {
		panic(err)
	}
	writeContext(ctx, clt)
	//readContext(ctx, clt)
	//writeContextError(ctx, clt)
}

func writeContext(ctx context.Context, clt *client.Client) {
	err := clt.WriteContext(ctx, "test.write", []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339))))
	if err != nil {
		panic(err)
	}
}

func readContext(ctx context.Context, clt *client.Client) {
	msgs, err := clt.ReadContext(ctx, "test.client", "test.topic", 1, 0)
	if err != nil {
		panic(err)
	}
	if len(msgs) == 0 {
		panic("no messages")
	}
	msg := msgs[0]
	log.Infof("recv: [%s]: %s", msg.Topic, string(msg.Data))
}
