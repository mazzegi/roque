package roque

import (
	"context"
	"time"

	"github.com/mazzegi/roque/message"
)

type Client interface {
	WriteContext(ctx context.Context, topic message.Topic, msgs ...[]byte) error
	ReadContext(ctx context.Context, clientID string, topic message.Topic, limit int, wait time.Duration) ([]message.Message, error)
	CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error
}
