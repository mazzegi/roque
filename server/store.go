package server

import (
	"github.com/mazzegi/roque/message"
	"github.com/mazzegi/roque/server/sqlite"
)

type Store interface {
	Append(msgs ...message.Message) error
	Commit(clientID string, topic string, idx int) error
	FetchNext(clientID string, topic string, limit int) ([]message.Message, error)
}

func NewSqliteStore(dsn string) (Store, error) {
	return sqlite.NewStore(dsn)
}
