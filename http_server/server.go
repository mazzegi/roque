package http_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/mazzegi/log"
	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/joinctx"
	"github.com/mazzegi/roque/message"
)

func New(bind string, prefix string, disp *roque.Dispatcher) (*Server, error) {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("listen to %q: %w", bind, err)
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	s := &Server{
		listener:   l,
		router:     mux.NewRouter(),
		prefix:     prefix,
		dispatcher: disp,
	}
	return s, nil
}

type Server struct {
	listener   net.Listener
	router     *mux.Router
	prefix     string
	dispatcher *roque.Dispatcher
}

func (s *Server) RunContext(rctx context.Context) {
	s.router.HandleFunc(s.prefix+"write", func(w http.ResponseWriter, r *http.Request) {
		s.handlePOSTWrite(rctx, w, r)
	}).Methods("POST")
	s.router.HandleFunc(s.prefix+"read", func(w http.ResponseWriter, r *http.Request) {
		s.handleGETRead(rctx, w, r)
	}).Methods("GET")
	s.router.HandleFunc(s.prefix+"commit", func(w http.ResponseWriter, r *http.Request) {
		s.handlePOSTCommit(rctx, w, r)
	}).Methods("POST")

	srv := http.Server{
		Handler: s.router,
	}
	go srv.Serve(s.listener)

	<-rctx.Done()
	sdctx, sdcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sdcancel()
	srv.Shutdown(sdctx)
}

func (s *Server) handlePOSTWrite(rctx context.Context, w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		http.Error(w, "topic may not be empty", http.StatusBadRequest)
		return
	}
	var msgs [][]byte
	err := json.NewDecoder(r.Body).Decode(&msgs)
	if err != nil {
		http.Error(w, fmt.Sprintf("decode json failed: %v", err), http.StatusBadRequest)
		return
	}
	jctx, jcancel := joinctx.Join(rctx, r.Context())
	defer jcancel()
	err = s.dispatcher.WriteContext(jctx, topic, msgs...)
	if err != nil {
		log.Errorf("dispatcher-write: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGETRead(rctx context.Context, w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		http.Error(w, "topic may not be empty", http.StatusBadRequest)
		return
	}
	clientid := strings.TrimSpace(r.URL.Query().Get("clientid"))
	if clientid == "" {
		http.Error(w, "clientid may not be empty", http.StatusBadRequest)
		return
	}
	limit := 10
	slimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if slimit != "" {
		v, err := strconv.Atoi(slimit)
		if err != nil {
			http.Error(w, fmt.Sprintf("limit: cannot parse %q as int", slimit), http.StatusBadRequest)
			return
		}
		limit = v
	}
	wait := 30 * time.Second
	swait := strings.TrimSpace(r.URL.Query().Get("wait"))
	if swait != "" {
		v, err := strconv.Atoi(swait)
		if err != nil {
			http.Error(w, fmt.Sprintf("wait: cannot parse %q as int", swait), http.StatusBadRequest)
			return
		}
		wait = time.Millisecond * time.Duration(v)
	}

	jctx, jcancel := joinctx.Join(rctx, r.Context())
	defer jcancel()
	msgs, err := s.dispatcher.ReadContext(jctx, clientid, message.Topic(topic), limit, wait)
	if err != nil {
		log.Errorf("dispatcher-read: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handlePOSTCommit(rctx context.Context, w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		http.Error(w, "topic may not be empty", http.StatusBadRequest)
		return
	}
	clientid := strings.TrimSpace(r.URL.Query().Get("clientid"))
	if clientid == "" {
		http.Error(w, "clientid may not be empty", http.StatusBadRequest)
		return
	}
	sidx := strings.TrimSpace(r.URL.Query().Get("idx"))
	if sidx == "" {
		http.Error(w, "sidx may not be empty", http.StatusBadRequest)
		return
	}
	idx, err := strconv.Atoi(sidx)
	if err != nil {
		http.Error(w, fmt.Sprintf("idx: cannot parse %q as int", sidx), http.StatusBadRequest)
		return
	}
	jctx, jcancel := joinctx.Join(rctx, r.Context())
	defer jcancel()
	err = s.dispatcher.CommitContext(jctx, clientid, message.Topic(topic), idx)
	if err != nil {
		log.Errorf("dispatcher-commit: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
