package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/grpc_client"
	"github.com/mazzegi/roque/grpc_server"
	"github.com/mazzegi/roque/http_client"
	"github.com/mazzegi/roque/http_server"
	"github.com/mazzegi/roque/message"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	store, err := roque.NewSqliteStore(":memory:")
	if err != nil {
		panic(err)
	}
	disp := roque.NewDispatcher(store)

	var writeclt roque.Client
	var readclt roque.Client
	var transport string = "http"
	switch transport {
	case "grpc":
		writeclt = mustCreateGRPCClient(ctx)
		readclt = mustCreateGRPCClient(ctx)
		go runServerGRPC(ctx, disp)
	case "http":
		writeclt = mustCreateHTTPClient(ctx)
		readclt = mustCreateHTTPClient(ctx)
		go runServerHTTP(ctx, disp)
	default:
		panic("unknown transport " + transport)
	}
	time.Sleep(1 * time.Second)

	topic := message.Topic("test.roundtrip")
	clientID := "rountrip.client"

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("message-%d", 1000+rand.Intn(1000))
		//doneC := make(chan bool)
		t0 := time.Now()
		go func() {
			//defer close(doneC)
			writeclt.WriteContext(ctx, topic, []byte(msg))
		}()

		msgs, err := readclt.ReadContext(ctx, clientID, topic, 1, 10*time.Second)
		t1 := time.Now()
		if err != nil {
			panic(err)
		}
		if len(msgs) != 1 {
			panic("not 1 message")
		}
		rm := msgs[0]
		if string(rm.Data) != msg {
			panic("data not equal")
		}
		err = readclt.CommitContext(ctx, clientID, topic, rm.Index)
		if err != nil {
			panic(err)
		}
		fmt.Printf("roundtrip: %s\n", t1.Sub(t0))
		//<-doneC
	}
}

func mustCreateGRPCClient(ctx context.Context) roque.Client {
	clt, err := grpc_client.DialContext(ctx, "127.0.0.1:7001")
	if err != nil {
		panic(fmt.Errorf("grpc.dial: %w", err))
	}
	return clt
}

func mustCreateHTTPClient(ctx context.Context) roque.Client {
	clt := http_client.New("http://127.0.0.1:8080/roque/")
	return clt
}

func runServerGRPC(ctx context.Context, disp *roque.Dispatcher) {
	srv, err := grpc_server.New(":7001", disp)
	if err != nil {
		panic(err)
	}
	srv.RunContext(ctx)
}

func runServerHTTP(ctx context.Context, disp *roque.Dispatcher) {
	srv, err := http_server.New(":8080", "/roque/", disp)
	if err != nil {
		panic(err)
	}
	srv.RunContext(ctx)
}
