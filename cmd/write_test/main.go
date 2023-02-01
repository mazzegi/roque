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

	topic := "test.topic"
	count := 100
	log.Infof("writing %d messages to %q", count, topic)
	var msgs [][]byte
	for i := 0; i < count; i++ {
		msgs = append(msgs, []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339Nano))))
	}
	err = clt.WriteContext(ctx, topic, msgs...)
	if err != nil {
		panic(err)
	}
	log.Infof("wrote %d messages to %q", count, topic)
}
