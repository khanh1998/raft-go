package main

import (
	"flag"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	log := zerolog.New(output).With().Timestamp().Logger()

	log.Info().Str("foo", "bar").Msg("Hello World")

	// Output: 2006-01-02T15:04:05Z07:00 | INFO  | ***Hello World**** foo:BAR

	peers := []common.PeerInfo{
		{
			ID:      1,
			RpcUrl:  "localhost:1234",
			HttpUrl: "localhost:8080",
		},
		{
			ID:      2,
			RpcUrl:  "localhost:1235",
			HttpUrl: "localhost:8081",
		},
		{
			ID:      3,
			RpcUrl:  "localhost:1236",
			HttpUrl: "localhost:8082",
		},
	}

	id := flag.Int("id", -1, "")
	flag.Parse()
	minRandom, maxRandom := int64(1000), int64(3000)
	params := []node.NewNodeParams{
		{
			ID: 1,
			Brain: logic.NewRaftBrainParams{
				DataFileName:      "log.1.dat",
				MinRandomDuration: minRandom,
				MaxRandomDuration: maxRandom,
				Log:               &log,
				Peers:             peers,
				DB:                persistance.NewPersistence("log.1.dat"),
				StateMachine:      common.NewKeyValueStateMachine(),
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostURL: "localhost:1234",
				Log:     &log,
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8080",
			},
		},
		{
			ID: 2,
			Brain: logic.NewRaftBrainParams{
				DataFileName:      "log.2.dat",
				MinRandomDuration: minRandom,
				MaxRandomDuration: maxRandom,
				Log:               &log,
				Peers:             peers,
				DB:                persistance.NewPersistence("log.2.dat"),
				StateMachine:      common.NewKeyValueStateMachine(),
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostURL: "localhost:1235",
				Log:     &log,
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8081",
			},
		},
		{
			ID: 3,
			Brain: logic.NewRaftBrainParams{
				DataFileName:      "log.3.dat",
				MinRandomDuration: minRandom,
				MaxRandomDuration: maxRandom,
				Log:               &log,
				Peers:             peers,
				DB:                persistance.NewPersistence("log.3.dat"),
				StateMachine:      common.NewKeyValueStateMachine(),
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers:   peers,
				HostURL: "localhost:1236",
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
		n := node.NewNode(params[*id])
		n.Start()
	} else {
		// single process mode.
		// one process carry multiple raft instances.
		count := sync.WaitGroup{}
		for id, conf := range params {
			count.Add(1)
			go func(id int, conf node.NewNodeParams) {
				log.Info().Interface("nodes", id).Msg("start")
				n := node.NewNode(conf)
				n.Start()
				nodes[id] = n
				count.Done()

				log.Info().Interface("nodes", id).Msg("done")
			}(id, conf)
		}
		count.Wait()

		log.Info().Interface("nodes", nodes).Msg("cluster is created")
	}

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	log.Info().Msg("Shut down")
}
