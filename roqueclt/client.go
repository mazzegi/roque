package roqueclt

import (
	"context"
	"fmt"
	"io"

	"github.com/mazzegi/roque/roquemsg"
	"github.com/mazzegi/roque/roqueproto"
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
		roqueClt: roqueproto.NewRoqueClient(conn),
	}, nil
}

type Client struct {
	closer   io.Closer
	roqueClt roqueproto.RoqueClient
}

func (clt *Client) Close() {
	clt.closer.Close()
}

func (clt *Client) WriteContext(ctx context.Context, msgs ...roquemsg.Message) error {
	wc, err := clt.roqueClt.Write(ctx)
	if err != nil {
		return fmt.Errorf("roqueclt.write: %w", err)
	}
	for _, m := range msgs {
		err := wc.Send(&roqueproto.WriteRequest{Message: roquemsg.ToProto(m)})
		if err != nil {
			return fmt.Errorf("roqueclt.write.send: %w", err)
		}
	}
	res, err := wc.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("roqueclt.closeandrecv: %w", err)
	}
	if res.Status != roqueproto.RequestStatus_STATUS_OK {
		return fmt.Errorf("write.status: %s", res.StatusText)
	}
	return nil
}

func (clt *Client) ReadContext(ctx context.Context, clientID string, topic roquemsg.Topic) (roquemsg.Message, error) {
	rc, err := clt.roqueClt.Read(ctx, &roqueproto.ReadRequest{
		ClientID: clientID,
		Topic:    string(topic),
	})
	if err != nil {
		return roquemsg.Message{}, fmt.Errorf("roqueclt.read: %w", err)
	}
	res, err := rc.Recv()
	if err != nil {
		return roquemsg.Message{}, fmt.Errorf("roqueclt.read.recv: %w", err)
	}
	if res.Status != roqueproto.RequestStatus_STATUS_OK {
		return roquemsg.Message{}, fmt.Errorf("roqueclt.read.recv.status: %s", res.StatusText)
	}
	return roquemsg.FromProto(res.Message), nil
}

func (clt *Client) StreamContext(ctx context.Context, clientID string, topic roquemsg.Topic) (<-chan roquemsg.Message, error) {
	sc, err := clt.roqueClt.Stream(ctx, &roqueproto.ReadRequest{
		ClientID: clientID,
		Topic:    string(topic),
	})
	if err != nil {
		return nil, fmt.Errorf("roqueclt.stream: %w", err)
	}

	msgC := make(chan roquemsg.Message)
	go func() {
		defer close(msgC)
		for res, err := sc.Recv(); err == nil; res, err = sc.Recv() {
			msgC <- roquemsg.FromProto(res.Message)
		}
	}()
	return msgC, nil
}
