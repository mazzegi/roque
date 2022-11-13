package roquesrv

import (
	"context"
	"fmt"
	"net"

	"github.com/mazzegi/log"
	"github.com/mazzegi/roque/roquemsg"
	"github.com/mazzegi/roque/roqueproto"
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
	roqueproto.UnimplementedRoqueServer
}

func (s *Server) RunContext(ctx context.Context) {
	grpcSrv := grpc.NewServer()
	roqueproto.RegisterRoqueServer(grpcSrv, s)

	go func() {
		grpcSrv.Serve(s.listener)
	}()
	<-ctx.Done()
	grpcSrv.GracefulStop()
}

//

func (s *Server) Write(in roqueproto.Roque_WriteServer) error {
	for wr, err := in.Recv(); err == nil; wr, err = in.Recv() {
		werr := s.dispatcher.WriteContext(in.Context(), roquemsg.FromProto(wr.Message))
		if werr != nil {
			in.SendAndClose(&roqueproto.Void{})
			return fmt.Errorf("dispatcher.write: %w", werr)
		}
	}
	in.SendAndClose(&roqueproto.Void{})
	return nil
}

func (s *Server) Read(ctx context.Context, in *roqueproto.ReadRequest) (*roqueproto.Message, error) {
	msg, err := s.dispatcher.ReadContext(ctx, in.ClientID, roquemsg.Topic(in.Topic))
	if err != nil {
		return nil, fmt.Errorf("dispatcher.read: %w", err)
	}
	return roquemsg.ToProto(msg), nil
}

func (s *Server) Stream(in *roqueproto.ReadRequest, out roqueproto.Roque_StreamServer) error {
	for {
		msg, err := s.dispatcher.ReadContext(out.Context(), in.ClientID, roquemsg.Topic(in.Topic))
		if err != nil {
			return fmt.Errorf("dispatcher.read: %w", err)
		}
		err = out.Send(roquemsg.ToProto(msg))
		if err != nil {
			log.Warnf("stream.out.send: %v", err)
			return fmt.Errorf("stream.out.send: %w", err)
		}
	}
}
