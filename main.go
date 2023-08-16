package main

import (
	"flag"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/rpc_proxy"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	log := zerolog.New(output).With().Timestamp().Logger()

	log.Info().Str("foo", "bar").Msg("Hello World")

	// Output: 2006-01-02T15:04:05Z07:00 | INFO  | ***Hello World**** foo:BAR

	peers := []common.PeerInfo{
		{
			ID:  1,
			URL: ":1234",
		},
		{
			ID:  2,
			URL: ":1235",
		},
		{
			ID:  3,
			URL: ":1236",
		},
	}

	id := flag.Int("id", -1, "")
	flag.Parse()
	params := []node.NewNodeParams{
		{
			Brain: logic.NewRaftBrainParams{
				ID:                1,
				DataFileName:      "log.1.dat",
				MinRandomDuration: 5000,
				MaxRandomDuration: 10000,
				Log:               &log,
				Peers:             peers,
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostID:  1,
				HostURL: ":1234",
				Log:     &log,
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8080",
			},
		},
		{
			Brain: logic.NewRaftBrainParams{
				ID:                2,
				DataFileName:      "log.2.dat",
				MinRandomDuration: 5000,
				MaxRandomDuration: 10000,
				Log:               &log,
				Peers:             peers,
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostID:  2,
				HostURL: ":1235",
				Log:     &log,
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8081",
			},
		},
		{
			Brain: logic.NewRaftBrainParams{
				ID:                3,
				DataFileName:      "log.3.dat",
				MinRandomDuration: 5000,
				MaxRandomDuration: 10000,
				Log:               &log,
				Peers:             peers,
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostID:  3,
				HostURL: ":1236",
				Log:     &log,
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8082",
			},
		},
	}

	nodes := make([]*node.Node, len(params))

	if id != nil && *id >= 0 {
		// multiple processes mode.
		// each process will carry a single raft instance.
		node.NewNode(params[*id])
	} else {
		// single process mode.
		// one process carry multiple raft instances.
		count := sync.WaitGroup{}
		for id, conf := range params {
			count.Add(1)
			go func(id int, conf node.NewNodeParams) {
				log.Info().Interface("nodes", id).Msg("start")
				n := node.NewNode(conf)
				nodes[id] = n
				count.Done()

				log.Info().Interface("nodes", id).Msg("done")
			}(id, conf)
		}
		count.Wait()
	}

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	log.Info().Msg("Shut down")
}
