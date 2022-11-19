package client

import (
	"context"
	"fmt"
	"io"
	"time"

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

func (clt *Client) WriteContext(ctx context.Context, topic string, msgs ...[]byte) error {
	_, err := clt.roqueClt.Write(ctx, &proto.WriteRequest{
		Topic:    topic,
		Messages: msgs,
	})
	if err != nil {
		return fmt.Errorf("roqueclt.write: %w", err)
	}
	return nil
}

func (clt *Client) ReadContext(ctx context.Context, clientID string, topic message.Topic, limit int, wait time.Duration) ([]message.Message, error) {
	resp, err := clt.roqueClt.Read(ctx, &proto.ReadRequest{
		ClientID: clientID,
		Topic:    string(topic),
		Limit:    int64(limit),
		WaitMSec: wait.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("roqueclt.read: %w", err)
	}
	return message.SliceFromProto(resp.Messages), nil
}

func (clt *Client) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	_, err := clt.roqueClt.Commit(ctx, &proto.CommitRequest{
		ClientID: clientID,
		Topic:    string(topic),
		Idx:      int64(idx),
	})
	if err != nil {
		return fmt.Errorf("roqueclt.read: %w", err)
	}
	return nil
}
