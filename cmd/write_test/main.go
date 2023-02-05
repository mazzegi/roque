package main

import (
	"context"
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

	topic := "test.topic"
	count := 100
	log.Infof("writing %d messages to %q", count, topic)
	var msgs [][]byte
	for i := 0; i < count; i++ {
		msgs = append(msgs, []byte(fmt.Sprintf("my time is %s", time.Now().Format(time.RFC3339Nano))))
	}
	t0 := time.Now()
	err = clt.WriteContext(ctx, message.Topic(topic), msgs...)
	if err != nil {
		panic(err)
	}
	log.Infof("wrote %d messages to %q in %s", count, topic, time.Since(t0))
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
