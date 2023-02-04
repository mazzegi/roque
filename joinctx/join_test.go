package joinctx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestJoin(t *testing.T) {
	{
		c1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
		c2, cancel2 := context.WithCancel(context.Background())

		jc, jcancel := Join(c1, c2)
		<-jc.Done()
		cancel1()
		cancel2()
		jcancel()

		err := jc.Err()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expect deadline-exceeded got %v", err)
		}
	}

	{
		c1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
		c2, cancel2 := context.WithCancel(context.Background())

		jc, jcancel := Join(c1, c2)
		time.AfterFunc(100*time.Millisecond, func() { cancel2() })
		<-jc.Done()
		cancel1()
		jcancel()

		err := jc.Err()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expect canceled got %v", err)
		}
	}

	{
		c1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
		c2, cancel2 := context.WithCancel(context.Background())

		jc, jcancel := Join(c1, c2)
		time.AfterFunc(100*time.Millisecond, func() { jcancel() })
		<-jc.Done()
		cancel1()
		cancel2()

		err := jc.Err()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expect canceled got %v", err)
		}
	}
}

func TestWait(t *testing.T) {
	//
	var cs []<-chan struct{}
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(i+1))
		defer cancel()
		cs = append(cs, ctx.Done())
	}

	t0 := time.Now()
	wait(cs...)
	fmt.Printf("elapsed: %s\n", time.Since(t0))
}
