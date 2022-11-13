package client

import (
	"context"
	"fmt"
	"io"

	"github.com/mazzegi/roque/message"
	"github.com/mazzegi/roque/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialContext(ctx context.Context, host string) (*Client, error) {
	conn, err := grpc.Dial(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.dial %q: %w", host, err)
	}
	return &Client{
		closer:   conn,
		roqueClt: proto.NewRoqueClient(conn),
	}, nil
}

type Client struct {
	closer   io.Closer
	roqueClt proto.RoqueClient
}

func (clt *Client) Close() {
	clt.closer.Close()
}

func (clt *Client) WriteContext(ctx context.Context, msgs ...message.Message) error {
	wc, err := clt.roqueClt.Write(ctx)
	if err != nil {
		return fmt.Errorf("roqueclt.write: %w", err)
	}
	for _, m := range msgs {
		err := wc.Send(&proto.WriteRequest{Message: message.ToProto(m)})
		if err != nil {
			return fmt.Errorf("roqueclt.write.send: %w", err)
		}
	}
	_, err = wc.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("roqueclt.closeandrecv: %w", err)
	}
	return nil
}

func (clt *Client) ReadContext(ctx context.Context, clientID string, topic message.Topic) (message.Message, error) {
	msg, err := clt.roqueClt.Read(ctx, &proto.ReadRequest{
		ClientID: clientID,
		Topic:    string(topic),
	})
	if err != nil {
		return message.Message{}, fmt.Errorf("roqueclt.read: %w", err)
	}
	return message.FromProto(msg), nil
}

func (clt *Client) StreamContext(ctx context.Context, clientID string, topic message.Topic) (<-chan message.Message, error) {
	sc, err := clt.roqueClt.Stream(ctx, &proto.ReadRequest{
		ClientID: clientID,
		Topic:    string(topic),
	})
	if err != nil {
		return nil, fmt.Errorf("roqueclt.stream: %w", err)
	}
	msgC := make(chan message.Message)
	go func() {
		defer close(msgC)
		for msg, err := sc.Recv(); err == nil; msg, err = sc.Recv() {
			msgC <- message.FromProto(msg)
		}
	}()
	return msgC, nil
}
