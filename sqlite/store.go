package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/mazzegi/roque/message"
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
	_, err := s.db.Exec(`
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
	`)
	if err != nil {
		return fmt.Errorf("exec init: %w", err)
	}

	// prepare
	s.stmtAppend, err = s.db.Prepare(`
		INSERT INTO messages (topic, topic_index, created_on, data) 
		VALUES (?,(SELECT COALESCE(MAX(topic_index)+1,0) AS topic_index FROM messages WHERE topic = ?),?,?);
	`)
	if err != nil {
		return fmt.Errorf("prepare stmt append: %w", err)
	}

	s.stmtFetch, err = s.db.Prepare(`
		SELECT topic_index, data
		FROM messages ms
		WHERE topic = ? AND topic_index > (SELECT COALESCE(MAX(topic_index),-1) FROM client_pointers WHERE client_id = ? AND topic = ?)
		ORDER BY topic_index ASC
		LIMIT ?;
	`)
	if err != nil {
		return fmt.Errorf("prepare stmt fetch: %w", err)
	}

	s.stmtCommit, err = s.db.Prepare(`
		REPLACE INTO client_pointers (client_id, topic, topic_index) VALUES (?,?,?);
	`)
	if err != nil {
		return fmt.Errorf("prepare stmt fetch: %w", err)
	}

	return nil
}

func (s *Store) Close() {
	s.stmtAppend.Close()
	s.stmtFetch.Close()
	s.stmtCommit.Close()
	s.db.Close()
}

type Store struct {
	sync.RWMutex
	db         *sql.DB
	stmtAppend *sql.Stmt
	stmtFetch  *sql.Stmt
	stmtCommit *sql.Stmt
}

func (s *Store) Append(topic string, msgs ...[]byte) error {
	s.Lock()
	defer s.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	stmt := tx.Stmt(s.stmtAppend)
	defer stmt.Close()
	for _, msg := range msgs {
		_, err := stmt.Exec(topic, topic, time.Now().UTC(), string(msg))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("exec: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *Store) Commit(clientID string, topic string, idx int) error {
	s.Lock()
	defer s.Unlock()
	_, err := s.stmtCommit.Exec(clientID, topic, idx)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func (s *Store) FetchNext(clientID string, topic string, limit int) ([]message.Message, error) {
	s.RLock()
	defer s.RUnlock()
	rows, err := s.stmtFetch.Query(topic, clientID, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var msgs []message.Message
	var idx int
	var data []byte
	for rows.Next() {
		err := rows.Scan(&idx, &data)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		msgs = append(msgs, message.Message{
			Topic: message.Topic(topic),
			Index: idx,
			Data:  data,
		})
	}
	return msgs, nil
}
