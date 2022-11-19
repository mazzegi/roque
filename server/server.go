package server

import (
	"context"
	"fmt"
	"net"

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
}

func (s *Server) RunContext(ctx context.Context) {
	grpcSrv := grpc.NewServer()
	proto.RegisterRoqueServer(grpcSrv, s)

	go func() {
		grpcSrv.Serve(s.listener)
	}()
	<-ctx.Done()
	grpcSrv.GracefulStop()
}

//

func (s *Server) Write(ctx context.Context, in *proto.WriteRequest) (*proto.Void, error) {
	err := s.dispatcher.WriteContext(ctx, message.SliceFromProto(in.Messages)...)
	if err != nil {
		return &proto.Void{}, fmt.Errorf("dispatcher.write: %w", err)
	}
	return &proto.Void{}, nil
}

func (s *Server) Read(ctx context.Context, in *proto.ReadRequest) (*proto.ReadResponse, error) {
	msgs, err := s.dispatcher.ReadContext(ctx, in.ClientID, message.Topic(in.Topic), int(in.Limit))
	if err != nil {
		return nil, fmt.Errorf("dispatcher.read: %w", err)
	}
	return &proto.ReadResponse{
		Messages: message.SliceToProto(msgs),
	}, nil
}

func (s *Server) Commit(ctx context.Context, in *proto.CommitRequest) (*proto.Void, error) {
	err := s.dispatcher.CommitContext(ctx, in.ClientID, message.Topic(in.Topic), int(in.Idx))
	if err != nil {
		return nil, fmt.Errorf("dispatcher.commit: %w", err)
	}
	return &proto.Void{}, nil
}
