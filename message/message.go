package message

import (
	"github.com/mazzegi/roque/proto"
	"golang.org/x/exp/slices"
)

type Topic string

type Message struct {
	Topic Topic
	Index int
	Data  []byte
}

func ToProto(msg Message) *proto.Message {
	return &proto.Message{
		Topic: string(msg.Topic),
		Idx:   int64(msg.Index),
		Data:  slices.Clone(msg.Data),
	}
}

func FromProto(msg *proto.Message) Message {
	return Message{
		Topic: Topic(msg.Topic),
		Index: int(msg.Idx),
		Data:  slices.Clone(msg.Data),
	}
}
