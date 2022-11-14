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

func (s *Store) Append(topic string, data []byte) error {
	s.Lock()
	defer s.Unlock()
	_, err := s.db.Exec(`		
		INSERT INTO messages (topic, topic_index, created_on, data) 
		VALUES (?,
			(SELECT COALESCE(MAX(topic_index)+1,0) AS topic_index FROM messages WHERE topic = ?),
			?,?);
	`, topic, topic, time.Now().UTC(), string(data))
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (s *Store) Commit(clientID string, topic string, idx int) error {
	s.Lock()
	defer s.Unlock()
	_, err := s.db.Exec("REPLACE INTO client_pointers (client_id, topic, topic_index) VALUES (?,?,?);",
		clientID, topic, idx)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (s *Store) FetchNext(clientID string, topic string) (data []byte, idx int, err error) {
	s.RLock()
	defer s.RUnlock()
	row := s.db.QueryRow(`		
		SELECT topic_index, data
		FROM messages ms
		WHERE topic_index > (SELECT COALESCE(MAX(topic_index),-1) FROM client_pointers WHERE client_id = ? AND topic = ?)
		ORDER BY topic_index ASC
		LIMIT 1
		;		
	`, clientID, topic)
	err = row.Scan(&idx, &data)
	if err != nil {
		return nil, -1, fmt.Errorf("scan: %w", err)
	}
	return data, idx, nil
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

CREATE TABLE IF NOT EXISTS client_pointers (
	client_id   	TEXT,
	topic	   		TEXT,
	topic_index     INTEGER,	
	PRIMARY KEY     (client_id, topic)
);
`
