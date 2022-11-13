package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/mazzegi/roque/roquesrv"
)

func main() {
	disp := roquesrv.NewDispatcher()
	srv, err := roquesrv.New(":7001", disp)
	if err != nil {
		panic(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	srv.RunContext(ctx)
}
