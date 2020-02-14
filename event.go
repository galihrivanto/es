package event

import (
	"context"
	"io"
)

// Message define payload of event
type Message struct {
	Body []byte
}

// Event contains transferable object of pub/sub
type Event interface {
	Topic() string
	Message() *Message

	// optionally ack message
	Ack() error
}

// Handler incoming event
type Handler func(event Event) error

// SubscribeResult define subscribe result, including option to unsubscribe
type SubscribeResult interface {
	Unsubscribe() error
}

// Broker define publisher / subscriber functionality
type Broker interface {
	io.Closer

	Open() error
	Publish(ctx context.Context, topic string, message *Message) error
	Subscribe(ctx context.Context, topic string, handler Handler) (SubscribeResult, error)
}
