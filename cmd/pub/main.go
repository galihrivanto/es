package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	ags "github.com/galihrivanto/ags/v3"
	event "github.com/galihrivanto/es"
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
	flag.StringVar(&filepath, "p", "", "ags file")
	flag.Parse()

	if filepath == "" {
		fmt.Println("file not specified")
		os.Exit(1)
	}

	broker := event.CreateNatsBroker(urls, clusterID, clientID)
	if err := broker.Open(); err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	parser := ags.NewParser(true)
	err := parser.ParseFromFile(filepath, func(pc *ags.ParseContext, values []string) error {
		if pc.Group == "MONR" {
			b, err := json.Marshal(values)
			if err != nil {
				return err
			}

			log.Println("publish", string(b))

			return broker.Publish(context.Background(), topic, &event.Message{Body: b})
		}

		return nil
	})
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	fmt.Println("done!!")
}
