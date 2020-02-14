package event

import (
	"context"
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type publication struct {
	t string
	m *Message
}

func (p *publication) Topic() string {
	return p.t
}

func (p *publication) Message() *Message {
	return p.m
}

func (p *publication) Ack() error {
	// nats does not support acking
	return nil
}

type NatsBroker struct {
	urls        string
	clusterName string
	clientName  string

	opened bool
	nc     *nats.Conn
	sc     stan.Conn
}

func (b *NatsBroker) Open() error {
	opts := []nats.Option{nats.Name("Even sourcing experiment")}

	nc, err := nats.Connect(b.urls, opts...)
	if err != nil {
		return err
	}

	sc, err := stan.Connect(b.clusterName, b.clientName, stan.NatsConn(nc))
	if err != nil {
		// close nats connection if streaming connection fails
		nc.Close()

		return err
	}

	b.nc = nc
	b.sc = sc
	b.opened = true

	return nil
}

func (b *NatsBroker) Close() error {
	defer b.nc.Close()

	b.opened = false
	return b.sc.Close()
}

func (b *NatsBroker) Publish(ctx context.Context, topic string, message *Message) error {
	if !b.opened {
		if err := b.Open(); err != nil {
			return err
		}
	}

	e := &publication{
		t: topic,
		m: message,
	}

	data, err := json.Marshal(e.Message())
	if err != nil {
		return err
	}

	return b.sc.Publish(topic, data)
}

func (b *NatsBroker) Subscribe(ctx context.Context, topic string, handler Handler) (SubscribeResult, error) {
	if !b.opened {
		if err := b.Open(); err != nil {
			return nil, err
		}
	}

	if handler == nil {
		// set default handler
		handler = func(event Event) error {
			log.Println("event:", event)
			return nil
		}
	}

	fn := func(m *stan.Msg) {
		var msg *Message
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			// drop message
			log.Println("failed to unmarshal event", err)
			return
		}

		handler(&publication{t: topic, m: msg})
	}

	log.Println("subscribe", topic)

	subscription, err := b.sc.QueueSubscribe(topic, "", fn, stan.DeliverAllAvailable(), stan.DurableName(""))
	if err != nil {
		return nil, err
	}

	return subscription, nil
}

func CreateNatsBroker(urls, clusterName, clientName string) Broker {
	b := &NatsBroker{
		urls:        urls,
		clusterName: clusterName,
		clientName:  clientName,
	}

	return b
}
