package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/grpc_client"
	"github.com/mazzegi/roque/http_client"
	"github.com/mazzegi/roque/message"
)

func main() {
	topic := flag.String("topic", "test.topic", "topic to read from")
	clientID := flag.String("client", "test.client", "clinet id")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	var clt roque.Client
	var err error
	var transport string = "http"
	switch transport {
	case "grpc":
		clt, err = createGRPCClient(ctx)
	case "http":
		clt, err = createHTTPClient(ctx)
	default:
		panic("unknown transport " + transport)
	}
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

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
			continue
		}
		for _, msg := range msgs {
			log.Infof("recv: [%s:%d]: %s", msg.Topic, msg.Index, string(msg.Data))
		}
		lastMsg := msgs[len(msgs)-1]
		clt.CommitContext(ctx, *clientID, lastMsg.Topic, lastMsg.Index)
	}
}

func createGRPCClient(ctx context.Context) (roque.Client, error) {
	clt, err := grpc_client.DialContext(ctx, "127.0.0.1:7001")
	if err != nil {
		return nil, fmt.Errorf("grpc.dial: %w", err)
	}
	return clt, nil
}

func createHTTPClient(ctx context.Context) (roque.Client, error) {
	clt := http_client.New("http://127.0.0.1:8080/roque/")
	return clt, nil
}
