package roquemsg

import (
	"github.com/mazzegi/roque"
	"github.com/mazzegi/roque/roqueproto"
)

type Topic string

type Message struct {
	Topic Topic
	Data  []byte
}

func ToProto(msg Message) *roqueproto.Message {
	return &roqueproto.Message{
		Topic: string(msg.Topic),
		Data:  roque.SliceClone(msg.Data),
	}
}

func FromProto(msg *roqueproto.Message) Message {
	return Message{
		Topic: Topic(msg.Topic),
		Data:  roque.SliceClone(msg.Data),
	}
}
