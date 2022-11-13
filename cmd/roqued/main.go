package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/mazzegi/roque/server"
)

func main() {
	disp := server.NewDispatcher()
	srv, err := server.New(":7001", disp)
	if err != nil {
		panic(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	srv.RunContext(ctx)
}
