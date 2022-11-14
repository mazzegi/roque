package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

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

	count := 100
	for i := 0; i < count; i++ {
		err = clt.WriteContext(ctx, message.Message{
			Topic: "test.topic",
			Data:  []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339Nano))),
		})
		if err != nil {
			panic(err)
		}
	}
}
