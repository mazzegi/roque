package server

import (
	"context"
	"sync"
	"time"
)

func NewSignal() *Signal {
	return &Signal{
		subs: make(map[string]map[chan bool]bool),
	}
}

type Signal struct {
	sync.RWMutex
	subs map[string]map[chan bool]bool
}

func (s *Signal) Emit(topic string) {
	s.Lock()
	defer s.Unlock()
	for c := range s.subs[topic] {
		close(c)
	}
	delete(s.subs, topic)
}

func (s *Signal) subscribe(topic string) chan bool {
	s.Lock()
	defer s.Unlock()
	c := make(chan bool)
	if _, ok := s.subs[topic]; !ok {
		s.subs[topic] = make(map[chan bool]bool)
	}
	s.subs[topic][c] = true
	return c
}

func (s *Signal) unsubscribe(topic string, c chan bool) {
	s.Lock()
	defer s.Unlock()
	cs, ok := s.subs[topic]
	if !ok {
		return
	}
	delete(cs, c)
}

func (s *Signal) WaitContext(ctx context.Context, topic string, timeout time.Duration) bool {
	c := s.subscribe(topic)
	defer s.unsubscribe(topic, c)
	timer := time.NewTimer(timeout)
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	case <-c:
		return true
	}
}
