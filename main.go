package main

import (
	"flag"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/rpc_proxy"
	"os"
	"os/signal"
	"syscall"
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
	params := []node.NewNodeParams{
		{
			Brain: logic.NewRaftBrainParams{
				ID:                1,
				DataFileName:      "log.1.dat",
				MinRandomDuration: 5000,
				MaxRandomDuration: 10000,
				Log:               &log,
				Peers: []logic.PeerInfo{
					{
						ID:  2,
						URL: ":1235",
					},
					{
						ID:  3,
						URL: ":1236",
					},
				},
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers: []rpc_proxy.PeerRPCProxyConnectInfo{
					{
						ID:  2,
						URL: ":1235",
					},
					{
						ID:  3,
						URL: ":1236",
					},
				},
				HostID:  1,
				HostURL: ":1234",
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
				Peers: []logic.PeerInfo{
					{
						ID:  1,
						URL: ":1234",
					},
					{
						ID:  3,
						URL: ":1236",
					},
				},
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers: []rpc_proxy.PeerRPCProxyConnectInfo{
					{
						ID:  1,
						URL: ":1234",
					},
					{
						ID:  3,
						URL: ":1236",
					},
				},
				HostID:  2,
				HostURL: ":1235",
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
				Peers: []logic.PeerInfo{
					{
						ID:  1,
						URL: ":1234",
					},
					{
						ID:  2,
						URL: ":1235",
					},
				},
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				Peers: []rpc_proxy.PeerRPCProxyConnectInfo{
					{
						ID:  1,
						URL: ":1234",
					},
					{
						ID:  2,
						URL: ":1235",
					},
				},
				HostID:  3,
				HostURL: ":1236",
			},
			HTTPProxy: http_proxy.NewHttpProxyParams{
				URL: "localhost:8082",
			},
		},
	}
	signChan := make(chan os.Signal, 1)

	node.NewNode(params[*id])

	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	log.Info().Msg("Shut down")
}
