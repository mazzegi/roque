package roque

import (
	"github.com/mazzegi/roque/message"
	"github.com/mazzegi/roque/sqlite"
)

type Store interface {
	Append(topic string, msgs ...[]byte) error
	Commit(clientID string, topic string, idx int) error
	FetchNext(clientID string, topic string, limit int) ([]message.Message, error)
}

func NewSqliteStore(dsn string) (Store, error) {
	return sqlite.NewStore(dsn)
}
