package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"khanh/raft-go/common"
	classicCm "khanh/raft-go/extensions/classic/common"
	classicHttp "khanh/raft-go/extensions/classic/http_server"
	classicSt "khanh/raft-go/extensions/classic/state_machine"
	etcdCm "khanh/raft-go/extensions/etcd/common"
	etcdHttp "khanh/raft-go/extensions/etcd/http_server"
	etcdSt "khanh/raft-go/extensions/etcd/state_machine"
	"khanh/raft-go/node"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/otel"
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
	// each cluster can have at most one node start with catching-up = false, this is the first node of the cluster,
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
	ctx := context.Background()
	tracer := otel.Tracer("main.go")
	ctx, span := tracer.Start(ctx, "main.go")

	config, err := common.ReadConfigFromFile(nil)
	if err != nil {
		log.Panic(err)
	}

	// current server information
	var (
		id              int
		httpUrl, rpcUrl string
		clusterMembers  []common.ClusterMember
		logFactory      common.LogFactory
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
		rpcUrl = fmt.Sprintf(":%d", *rpcPort)

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

	logger := observability.NewZerolog(config.Observability, id)

	logger.InfoContext(ctx, "config content", "config", config)

	dataFolder := fmt.Sprintf("%s%d/", config.DataFolder, id)

	storage, err := storage.NewStorage(storage.NewStorageParams{
		WalSize:    config.WalSizeLimit,
		DataFolder: dataFolder,
		Logger:     logger,
	}, storage.FileWrapperImpl{})
	if err != nil {
		logger.Fatal("new storage", "error", err.Error())
	}

	switch config.LogExtensions.Enable {
	case common.LogExtensionClassic:
		logFactory = classicCm.ClassicLogFactory{
			NewSnapshot: classicSt.NewClassicSnapshotI,
		}
	case common.LogExtensionEtcd:
		logFactory = etcdCm.EtcdLogFactory{
			NewSnapshot: func() common.Snapshot {
				degree := config.LogExtensions.Etcd.StateMachineBTreeDegree
				return etcdSt.NewEtcdSnapshot(degree)
			},
		}
	}

	snapshot, raftPersistState, tmpClusterMembers, err := persistence_state.Deserialize(ctx, storage, config.Cluster.Mode, logger, logFactory)
	if err != nil {
		logger.Fatal("Deserialize system", "error", err.Error())
	}
	raftPersistState.SetStorage(storage)

	// dynamic cluster member configurations are read from persisted logs and snapshot
	if config.Cluster.Mode == common.Dynamic && len(clusterMembers) == 0 {
		clusterMembers = tmpClusterMembers
	}

	params := node.NewNodeParams{
		ID: id,
		Brain: logic.NewRaftBrainParams{
			ID:                  id,
			Mode:                config.Cluster.Mode,
			HeartBeatTimeOutMin: config.MinHeartbeatTimeout,
			HeartBeatTimeOutMax: config.MaxHeartbeatTimeout,
			ElectionTimeOutMin:  config.MinElectionTimeout,
			ElectionTimeOutMax:  config.MaxElectionTimeout,
			Logger:              logger,
			Members:             clusterMembers,
			CachingUp:           catchingUp,
			RpcRequestTimeout:   config.RpcRequestTimeout,
			PersistenceState:    raftPersistState,
			LogLengthLimit:      config.LogLengthLimit,
			SnapshotChunkSize:   config.SnapshotChunkSize,
			LogFactory:          logFactory,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostID:               id,
			HostURL:              rpcUrl,
			Logger:               logger,
			RpcDialTimeout:       config.RpcDialTimeout,
			RpcRequestTimeout:    config.RpcRequestTimeout,
			RpcReconnectDuration: config.RpcReconnectDuration,
		},
		ClassicSetup: &node.ClassicSetup{
			HttpServer: classicHttp.NewClassicHttpProxyParams{
				URL:    httpUrl,
				Logger: logger,
			},
			StateMachine: classicSt.NewClassicStateMachineParams{
				ClientSessionDuration: uint64(config.LogExtensions.Classic.ClientSessionDuration.Nanoseconds()),
				Logger:                logger,
				PersistState:          raftPersistState,
				Snapshot:              snapshot,
			},
		},
		EtcdSetup: &node.EtcdSetup{
			HttpServer: etcdHttp.NewEtcdHttpProxyParams{
				URL:    httpUrl,
				Logger: logger,
			},
			StateMachine: etcdSt.NewBtreeKvStateMachineParams{
				Logger:                logger,
				PersistenceState:      raftPersistState,
				ResponseCacheCapacity: config.LogExtensions.Etcd.StateMachineHistoryCapacity,
				BtreeDegree:           config.LogExtensions.Etcd.StateMachineBTreeDegree,
				Snapshot:              snapshot,
			},
		},
		LogExtensionEnabled: config.LogExtensions.Enable,
		Logger:              logger,
	}

	// Set up OpenTelemetry.
	if !config.Observability.Disabled {
		otelShutdown, err := observability.SetupOTelSDK(ctx, id, config.Observability)
		if err != nil {
			log.Panic(err)
		}

		defer func() {
			err = errors.Join(err, otelShutdown(ctx))
			log.Panic(err)
		}()
	}

	n := node.NewNode(ctx, params)
	n.Start(ctx, params.Brain.Mode == common.Dynamic, params.Brain.CachingUp)

	span.End() // end init node

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan
	logger.Info("shutdown")
}
