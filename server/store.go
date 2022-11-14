package server

import "github.com/mazzegi/roque/server/sqlite"

type Store interface {
	Append(topic string, data []byte) error
	Commit(clientID string, topic string, idx int) error
	FetchNext(clientID string, topic string) (data []byte, idx int, err error)
}

func NewSqliteStore(dsn string) (Store, error) {
	return sqlite.NewStore(dsn)
}
