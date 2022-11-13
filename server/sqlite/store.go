package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dsn, err)
	}
	s := &Store{
		db: db,
	}
	err = s.init()
	if err != nil {
		s.db.Close()
		return nil, fmt.Errorf("init: %w", err)
	}
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(stmtInit)
	if err != nil {
		return fmt.Errorf("exec init: %w", err)
	}
	return nil
}

func (s *Store) Close() {
	s.db.Close()
}

type Store struct {
	sync.RWMutex
	db *sql.DB
}

func (s *Store) topicVersion(topic string) uint64 {
	row := s.db.QueryRow("SELECT MAX(topic_index)+1 FROM messages WHERE topic = ?;", topic)
	var ver uint64
	err := row.Scan(&ver)
	if err != nil {
		return 0
	}
	return ver
}

func (s *Store) Append(topic string, data []byte) error {
	s.Lock()
	defer s.Unlock()
	ver := s.topicVersion(topic)
	_, err := s.db.Exec("INSERT INTO messages (topic, topic_index, created_on, data) VALUES (?,?,?,?)",
		topic, ver, time.Now().UTC(), string(data))
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

//

const stmtInit = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous = OFF;

CREATE TABLE IF NOT EXISTS messages (	
	topic	   		TEXT,
	topic_index	    INTEGER,
	created_on		TEXT,
	data 			TEXT,
	PRIMARY KEY     (topic, topic_index)
);

CREATE TABLE IF NOT EXISTS clients (
	client_id   	TEXT,
	topic	   		TEXT,
	topic_pointer   INTEGER,	
	PRIMARY KEY     (client_id, topic)
);
`
