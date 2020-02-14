package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"syscall"

	event "github.com/galihrivanto/es"
	"github.com/galihrivanto/runner"
)

var (
	urls      string
	clusterID string
	clientID  string
	filepath  string
	topic     string
)

func main() {
	flag.StringVar(&urls, "u", "nats://127.0.0.1", "address of nats streaming server")
	flag.StringVar(&clusterID, "c", "test-cluster", "streaming cluster")
	flag.StringVar(&clientID, "cli", "test-client", "client id")
	flag.StringVar(&topic, "t", "ags.reading", "nats topic")
	flag.Parse()

	broker := event.CreateNatsBroker(urls, clusterID, clientID)
	if err := broker.Open(); err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	ctx := context.Background()
	fn := func(event event.Event) error {
		m := event.Message()
		values := []string{}

		if err := json.Unmarshal(m.Body, &values); err != nil {
			return err
		}

		log.Println("received", values)
		return nil
	}

	done := make(chan struct{}, 1)
	defer close(done)

	runner.
		Run(ctx, func(ctx context.Context) error {
			sub, err := broker.Subscribe(ctx, topic, fn)
			if err != nil {
				return err
			}

			log.Println("Listening on...", topic)

			<-done

			sub.Unsubscribe()

			return nil
		}).
		Handle(func(sig os.Signal) {
			if sig == syscall.SIGHUP {
				return
			}

			done <- struct{}{}

			log.Println("Shutting down...")
		})
}
