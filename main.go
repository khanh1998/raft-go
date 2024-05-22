package main

import (
	"flag"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	log := zerolog.New(output).With().Timestamp().Logger()

	id := flag.Int("id", -1, "")
	// if catchingUp is false, the node will be the follower in one-member-cluster. the follower then become the leader after that.
	// if catchingUP is true, the node will receive logs from leader. once the node is cautch up with the leader,
	// it will be add to the cluster as a follower.
	// each cluster can have at most one node start with catching-up = fase, this is the first node of the cluster,
	// following nodes have to set catching-up = true.
	catchingUp := flag.Bool("catching-up", false, "to specify whether the current node need to catch up with the leader or not")
	rpcPort := flag.Int("rpc-port", 1234, "RPC port")
	httpPort := flag.Int("http-port", 8080, "HTTP port")
	flag.Parse()

	if id == nil || *id < 0 {
		log.Fatal().Msg("node's `id` is required")
	}
	fileName := fmt.Sprintf("log.%d.dat", *id)

	params := node.NewNodeParams{
		ID: *id,
		Brain: logic.NewRaftBrainParams{
			DataFileName:        fileName,
			HeartBeatTimeOutMin: 2000,
			HeartBeatTimeOutMax: 5000,
			ElectionTimeOutMin:  12000,
			ElectionTimeOutMax:  15000,
			Log:                 &log,
			Info: common.ClusterMember{
				ID:      *id,
				RpcUrl:  fmt.Sprintf("localhost:%d", *rpcPort),
				HttpUrl: fmt.Sprintf("localhost:%d", *httpPort),
			},
			DB:           persistance.NewPersistence(fileName),
			StateMachine: common.NewKeyValueStateMachine(),
			CachingUp:    *catchingUp,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostURL: fmt.Sprintf("localhost:%d", *rpcPort),
			Log:     &log,
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL: fmt.Sprintf("localhost:%d", *httpPort),
		},
	}

	n := node.NewNode(params)
	n.Start(params.Brain.CachingUp)

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	log.Info().Msg("Shut down")
}
