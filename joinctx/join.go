package joinctx

import (
	"context"
	"errors"
	"sync"
	"time"
)

func wait(cs ...<-chan struct{}) {
	wc := make(chan struct{}, len(cs))
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	for _, c := range cs {
		wg.Add(1)
		go func(ctx context.Context, c <-chan struct{}) {
			defer wg.Done()
			select {
			case <-c:
				wc <- struct{}{}
			case <-ctx.Done():
			}

		}(ctx, c)
	}
	<-wc
	cancel()
	wg.Wait()
}

func Join(ctxBase context.Context, ctxExtra ...context.Context) (context.Context, context.CancelFunc) {
	jctx := joinedCtx{
		ctxs:    append([]context.Context{ctxBase}, ctxExtra...),
		doneC:   make(chan struct{}),
		cancelC: make(chan struct{}),
	}
	go jctx.run()
	return &jctx, func() { close(jctx.cancelC) }
}

type joinedCtx struct {
	ctxs    []context.Context
	doneC   chan struct{}
	cancelC chan struct{}
}

func (jctx *joinedCtx) run() {
	cs := []<-chan struct{}{jctx.cancelC}
	for _, c := range jctx.ctxs {
		cs = append(cs, c.Done())
	}
	wait(cs...)
	close(jctx.doneC)
}

func (jctx *joinedCtx) Deadline() (deadline time.Time, ok bool) {
	var minD *time.Time
	for _, c := range jctx.ctxs {
		if cd, ok := c.Deadline(); ok {
			if minD == nil || cd.Before(*minD) {
				minD = &cd
			}
		}
	}
	if minD == nil {
		return *minD, true
	}
	return time.Time{}, false
}

func (jctx *joinedCtx) Done() <-chan struct{} {
	return jctx.doneC
}

func (jctx *joinedCtx) Err() error {
	var rerr error
	for _, c := range jctx.ctxs {
		err := c.Err()
		if err == nil {
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		rerr = err
	}
	return rerr
}

func (jctx *joinedCtx) Value(key any) any {
	for _, c := range jctx.ctxs {
		if v := c.Value(key); v != nil {
			return v
		}
	}
	return nil
}
