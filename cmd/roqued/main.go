package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/grpc_server"
	"github.com/mazzegi/roque/http_server"
)

func main() {
	store, err := roque.NewSqliteStore("roque.db")
	if err != nil {
		panic(err)
	}
	disp := roque.NewDispatcher(store)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	var transport string = "http"
	switch transport {
	case "grpc":
		runGRPC(ctx, disp)
	case "http":
		runHTTP(ctx, disp)
	default:
		panic("unknown transport " + transport)
	}
}

func runGRPC(ctx context.Context, disp *roque.Dispatcher) {
	srv, err := grpc_server.New(":7001", disp)
	if err != nil {
		panic(err)
	}
	srv.RunContext(ctx)
}

func runHTTP(ctx context.Context, disp *roque.Dispatcher) {
	srv, err := http_server.New(":8080", "/roque/", disp)
	if err != nil {
		panic(err)
	}
	srv.RunContext(ctx)
}
