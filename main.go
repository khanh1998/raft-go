package main

import (
	"flag"
	"khanh/raft-go/logic"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	log := zerolog.New(output).With().Timestamp().Logger()

	log.Info().Str("foo", "bar").Msg("Hello World")

	// Output: 2006-01-02T15:04:05Z07:00 | INFO  | ***Hello World**** foo:BAR

	id := flag.Int("id", 0, "")
	flag.Parse()

	paramList := []logic.NewNodeParams{
		{
			ID:                1,
			PeerRpcURLs:       []string{":1235", ":1236"},
			RpcHostURL:        ":1234",
			RestApiHostURL:    "localhost:8080",
			DataFileName:      "log.1.dat",
			MinRandomDuration: 5000,
			MaxRandomDuration: 10000,
			Log:               &log,
		},
		{
			ID:                2,
			PeerRpcURLs:       []string{":1234", ":1236"},
			RpcHostURL:        ":1235",
			RestApiHostURL:    "localhost:8081",
			DataFileName:      "log.2.dat",
			MinRandomDuration: 50000,
			MaxRandomDuration: 100000,
			Log:               &log,
		},
		{
			ID:                3,
			PeerRpcURLs:       []string{":1234", ":1235"},
			RpcHostURL:        ":1236",
			RestApiHostURL:    "localhost:8082",
			DataFileName:      "log.3.dat",
			MinRandomDuration: 50000,
			MaxRandomDuration: 100000,
			Log:               &log,
		},
	}

	node, err := logic.NewNode(paramList[*id])
	if err != nil {
		log.Err(err).Msg("can not create new node")
	}

	log.Info().Msgf("node %d is created", paramList[*id].ID)

	<-node.Stop()
}
