package main

import (
	"flag"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

func readCmdArgsForStatic() (id *int, err error) {
	id = flag.Int("id", -1, "")
	// corresponding rpc port and http port will be get from the config file
	flag.Parse()
	if *id < 0 {
		return nil, fmt.Errorf("node's `id` is required")
	}
	return id, nil
}

func readCmdArgsForDynamic() (id *int, catchingUp *bool, httpPort *int, rpcPort *int, err error) {
	id = flag.Int("id", -1, "")
	// if catchingUp is false, the node will be the follower in one-member-cluster. the follower then become the leader after that.
	// if catchingUP is true, the node will receive logs from leader. once the node is cautch up with the leader,
	// it will be add to the cluster as a follower.
	// each cluster can have at most one node start with catching-up = fase, this is the first node of the cluster,
	// following nodes have to set catching-up = true.
	catchingUp = flag.Bool("catching-up", false, "to specify whether the current node need to catch up with the leader or not")
	rpcPort = flag.Int("rpc-port", -1, "RPC port")
	httpPort = flag.Int("http-port", -1, "HTTP port")

	flag.Parse()
	if *id < 0 {
		err = fmt.Errorf("node's `id` is required")
		return
	}

	if *rpcPort < 0 {
		err = fmt.Errorf("node's `rpcPort` is required")
		return
	}

	if *httpPort < 0 {
		err = fmt.Errorf("node's `httpPort` is required")
		return
	}

	return
}

func main() {
	config, err := common.ReadConfigFromFile(nil)
	if err != nil {
		log.Panic(err)
	}

	// current server information
	var (
		id              int
		httpUrl, rpcUrl string
		clusterMembers  []common.ClusterMember
	)

	if config.Cluster.Mode == common.Static {
		_id, err := readCmdArgsForStatic()
		if err != nil {
			log.Fatal(err)
		}

		for _, server := range config.Cluster.Servers {
			if server.ID == *_id {
				httpUrl = fmt.Sprintf("%s:%d", server.Host, server.HttpPort)
				rpcUrl = fmt.Sprintf("%s:%d", server.Host, server.RpcPort)
				id = server.ID
			}

			clusterMembers = append(clusterMembers, common.ClusterMember{
				ID:      server.ID,
				RpcUrl:  fmt.Sprintf("%s:%d", server.Host, server.RpcPort),
				HttpUrl: fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
			})
		}
	}

	var catchingUp bool

	if config.Cluster.Mode == common.Dynamic {
		_id, _catchingUp, httpPort, rpcPort, err := readCmdArgsForDynamic()
		if err != nil {
			log.Fatal(err)
		}

		id = *_id
		catchingUp = *_catchingUp
		httpUrl = fmt.Sprintf("localhost:%d", *httpPort)
		rpcUrl = fmt.Sprintf("localhost:%d", *rpcPort)

		if !catchingUp {
			clusterMembers = []common.ClusterMember{
				{
					ID:      id,
					RpcUrl:  rpcUrl,
					HttpUrl: httpUrl,
				},
			}
		}
	}

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	logger := zerolog.New(output).With().Timestamp().Logger()

	logger.Info().Interface("config", config).Msg("config")

	walFileName := fmt.Sprintf("log.%d.dat", id)
	dataFolder := fmt.Sprintf("%s/%d/", config.DataFolder, id)

	params := node.NewNodeParams{
		ID: id,
		Brain: logic.NewRaftBrainParams{
			ID:                  id,
			Mode:                config.Cluster.Mode,
			HeartBeatTimeOutMin: config.MinHeartbeatTimeoutMs,
			HeartBeatTimeOutMax: config.MaxHeartbeatTimeoutMs,
			ElectionTimeOutMin:  config.MinElectionTimeoutMs,
			ElectionTimeOutMax:  config.MaxElectionTimeoutMs,
			Logger:              &logger,
			Members:             clusterMembers,
			DB:                  common.NewPersistence(dataFolder, walFileName),
			CachingUp:           catchingUp,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostURL: rpcUrl,
			Logger:  &logger,
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL:    httpUrl,
			Logger: &logger,
		},
		StateMachine: state_machine.NewKeyValueStateMachineParams{
			DB:         common.NewPersistence(dataFolder, ""),
			DoSnapshot: config.StateMachineSnapshot,
		},
		Logger:     &logger,
		DataFolder: dataFolder,
	}

	n := node.NewNode(params)
	n.Start(params.Brain.Mode == common.Dynamic, params.Brain.CachingUp)

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	logger.Info().Msg("Shut down")
}
