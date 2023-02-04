package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/mazzegi/roque/joinctx"
	"github.com/mazzegi/roque/message"
	"github.com/mazzegi/roque/proto"
	"google.golang.org/grpc"
)

func New(bind string, disp *Dispatcher) (*Server, error) {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("listen to %q: %w", bind, err)
	}
	return &Server{
		listener:   l,
		dispatcher: disp,
	}, nil
}

type Server struct {
	listener   net.Listener
	dispatcher *Dispatcher
	proto.UnimplementedRoqueServer

	write  func(ctx context.Context, in *proto.WriteRequest) (*proto.Void, error)
	read   func(ctx context.Context, in *proto.ReadRequest) (*proto.ReadResponse, error)
	commit func(ctx context.Context, in *proto.CommitRequest) (*proto.Void, error)
}

func (s *Server) Write(ctx context.Context, in *proto.WriteRequest) (*proto.Void, error) {
	return s.write(ctx, in)
}
func (s *Server) Read(ctx context.Context, in *proto.ReadRequest) (*proto.ReadResponse, error) {
	return s.read(ctx, in)
}
func (s *Server) Commit(ctx context.Context, in *proto.CommitRequest) (*proto.Void, error) {
	return s.commit(ctx, in)
}

func (s *Server) RunContext(rctx context.Context) {
	s.write = func(ctx context.Context, in *proto.WriteRequest) (*proto.Void, error) {
		jctx, jcancel := joinctx.Join(rctx, ctx)
		defer jcancel()
		err := s.dispatcher.WriteContext(jctx, in.Topic, in.Messages...)
		if err != nil {
			return &proto.Void{}, fmt.Errorf("dispatcher.write: %w", err)
		}
		return &proto.Void{}, nil
	}
	s.read = func(ctx context.Context, in *proto.ReadRequest) (*proto.ReadResponse, error) {
		jctx, jcancel := joinctx.Join(rctx, ctx)
		defer jcancel()
		msgs, err := s.dispatcher.ReadContext(jctx, in.ClientID, message.Topic(in.Topic), int(in.Limit), time.Duration(in.WaitMSec)*time.Millisecond)
		if err != nil {
			return nil, fmt.Errorf("dispatcher.read: %w", err)
		}
		return &proto.ReadResponse{
			Messages: message.SliceToProto(msgs),
		}, nil
	}
	s.commit = func(ctx context.Context, in *proto.CommitRequest) (*proto.Void, error) {
		jctx, jcancel := joinctx.Join(rctx, ctx)
		defer jcancel()
		err := s.dispatcher.CommitContext(jctx, in.ClientID, message.Topic(in.Topic), int(in.Idx))
		if err != nil {
			return nil, fmt.Errorf("dispatcher.commit: %w", err)
		}
		return &proto.Void{}, nil
	}

	grpcSrv := grpc.NewServer()
	proto.RegisterRoqueServer(grpcSrv, s)

	go func() {
		grpcSrv.Serve(s.listener)
	}()
	<-rctx.Done()
	grpcSrv.GracefulStop()
}

//
