package node

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/extensions/etcd/http_server"
	"khanh/raft-go/extensions/etcd/state_machine"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
)

func PrepareNewNodeParams(ctx context.Context, id int, httpUrl, rpcUrl string, catchingUp bool, config *common.Config) (n NewNodeParams, err error) {
	clusterMembers := []gc.ClusterMember{}

	logger := observability.NewZerolog(config.Observability, id)
	logger.InfoContext(ctx, "config content", "config", config)

	if config.RaftCore.Cluster.Mode == gc.Static {
		for _, server := range config.RaftCore.Cluster.Servers {
			clusterMembers = append(clusterMembers, gc.ClusterMember{
				ID:      server.ID,
				RpcUrl:  fmt.Sprintf("%s:%d", server.Host, server.RpcPort),
				HttpUrl: fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
			})
		}
	}

	if config.RaftCore.Cluster.Mode == gc.Dynamic {
		if !catchingUp {
			clusterMembers = []gc.ClusterMember{
				{
					ID:      id,
					RpcUrl:  rpcUrl,
					HttpUrl: httpUrl,
				},
			}
		}
	}

	dataFolder := fmt.Sprintf("%s%d/", config.RaftCore.DataFolder, id)

	err = gc.CreateFolderIfNotExists(dataFolder)
	if err != nil {
		return n, fmt.Errorf("PrepareNewNodeParams CreateFolder: %w", err)
	}

	storage, err := storage.NewStorage(storage.NewStorageParams{
		WalSize:    config.RaftCore.WalSizeLimit,
		DataFolder: dataFolder,
		Logger:     logger,
	}, storage.FileWrapperImpl{})
	if err != nil {
		return n, fmt.Errorf("PrepareNewNodeParams NewStorage: %w", err)
	}

	logFactory := common.EtcdLogFactory{
		NewSnapshot: func() gc.Snapshot {
			degree := config.Extension.StateMachineBTreeDegree
			return state_machine.NewEtcdSnapshot(degree)
		},
	}

	snapshot, raftPersistState, tmpClusterMembers, err := persistence_state.Deserialize(ctx, storage, config.RaftCore.Cluster.Mode, logger, logFactory)
	if err != nil {
		return n, fmt.Errorf("PrepareNewNodeParams Deserialize: %w", err)
	}
	raftPersistState.SetStorage(storage)

	// dynamic cluster member configurations are read from persisted logs and snapshot
	if config.RaftCore.Cluster.Mode == gc.Dynamic && len(clusterMembers) == 0 {
		clusterMembers = tmpClusterMembers
	}

	params := NewNodeParams{
		ID: id,
		RaftCore: raft_core.NewRaftCoreParams{
			ID: id,
			Brain: logic.NewRaftBrainParams{
				ID:                  id,
				Mode:                config.RaftCore.Cluster.Mode,
				HeartBeatTimeOutMin: config.RaftCore.MinHeartbeatTimeout,
				HeartBeatTimeOutMax: config.RaftCore.MaxHeartbeatTimeout,
				ElectionTimeOutMin:  config.RaftCore.MinElectionTimeout,
				ElectionTimeOutMax:  config.RaftCore.MaxElectionTimeout,
				Logger:              logger,
				Members:             clusterMembers,
				CachingUp:           catchingUp,
				RpcRequestTimeout:   config.RaftCore.RpcRequestTimeout,
				PersistenceState:    raftPersistState,
				LogLengthLimit:      config.RaftCore.LogLengthLimit,
				SnapshotChunkSize:   config.RaftCore.SnapshotChunkSize,
				LogFactory:          logFactory,
			},
			RPCProxy: rpc_proxy.NewRPCImplParams{
				HostID:               id,
				HostURL:              rpcUrl,
				Logger:               logger,
				RpcDialTimeout:       config.RaftCore.RpcDialTimeout,
				RpcRequestTimeout:    config.RaftCore.RpcRequestTimeout,
				RpcReconnectDuration: config.RaftCore.RpcReconnectDuration,
			},
		},
		Extension: &EtcdExtParams{
			HttpServer: http_server.NewEtcdHttpProxyParams{
				URL:    httpUrl,
				Logger: logger,
			},
			StateMachine: state_machine.NewBtreeKvStateMachineParams{
				Logger:                logger,
				PersistenceState:      raftPersistState,
				ResponseCacheCapacity: config.Extension.StateMachineHistoryCapacity,
				BtreeDegree:           config.Extension.StateMachineBTreeDegree,
				Snapshot:              snapshot,
			},
		},
		Logger: logger,
	}

	return params, nil
}
