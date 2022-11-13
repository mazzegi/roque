package message

import (
	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/proto"
)

type Topic string

type Message struct {
	Topic Topic
	Data  []byte
}

func ToProto(msg Message) *proto.Message {
	return &proto.Message{
		Topic: string(msg.Topic),
		Data:  roque.SliceClone(msg.Data),
	}
}

func FromProto(msg *proto.Message) Message {
	return Message{
		Topic: Topic(msg.Topic),
		Data:  roque.SliceClone(msg.Data),
	}
}
