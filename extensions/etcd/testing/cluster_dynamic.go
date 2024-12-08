package testing

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"khanh/raft-go/extensions/etcd/http_server"
	"khanh/raft-go/extensions/etcd/state_machine"
	"khanh/raft-go/node"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
	"log"
)

// after a cluster is created, need to wait a moment so a follower can win a election and become leader
func NewDynamicCluster(filePath string) *Cluster {
	c := Cluster{}
	c.initDynamic(filePath)

	return &c
}

// dynamic cluster
func (c *Cluster) AddServer(ctx context.Context, id int) error {
	info, err := c.createNewNode(ctx, id)
	if err != nil {
		return err
	}
	return c.HttpAgent.AddMember(ctx, go_client.ClusterMemberRequest{
		ClusterMember: info,
	})
}

// dynamic cluster
func (c *Cluster) RemoveServerLeader(ctx context.Context) error {
	status, err := c.HasOneLeader()
	if err != nil {
		return err
	}

	id := status.ID

	return c.RemoveServer(ctx, id)
}

// dynamic cluster
func (c *Cluster) RemoveServer(ctx context.Context, id int) error {
	params := c.createNodeParams[id]
	return c.HttpAgent.RemoveMember(ctx, go_client.ClusterMemberRequest{
		ClusterMember: gc.ClusterMember{
			ID:      params.ID,
			RpcUrl:  params.RPCProxy.HostURL,
			HttpUrl: params.EtcdSetup.HttpServer.URL,
		},
	})
}

// for dynamic cluster
func (c *Cluster) createNewNode(ctx context.Context, id int) (info gc.ClusterMember, err error) {
	rpcUrl, httpUrl := fmt.Sprintf("localhost:%d", 1233+id), fmt.Sprintf("localhost:%d", 8079+id)
	info = gc.ClusterMember{ID: id, HttpUrl: httpUrl, RpcUrl: rpcUrl}
	catchingUp := id > 1

	dataFolder := fmt.Sprintf("%s%d/", c.config.DataFolder, id)

	storage, err := storage.NewStorage(storage.NewStorageParams{
		WalSize:    c.config.WalSizeLimit,
		DataFolder: dataFolder,
		Logger:     c.log,
	}, storage.FileWrapperImpl{})
	if err != nil {
		return info, err
	}

	logFactory := common.EtcdLogFactory{
		NewSnapshot: func() gc.Snapshot {
			return state_machine.NewEtcdSnapshot(c.config.LogExtensions.Etcd.StateMachineBTreeDegree)
		},
	}

	snapshot, raftPersistState, clusterMembers, err := persistence_state.Deserialize(ctx, storage, gc.Dynamic, c.log, logFactory)
	if err != nil {
		return info, err
	}

	raftPersistState.SetStorage(storage)

	param := node.NewNodeParams{
		ID: id,
		Brain: logic.NewRaftBrainParams{
			ID:                  id,
			Mode:                gc.Dynamic,
			CachingUp:           catchingUp,
			HeartBeatTimeOutMin: c.config.MinHeartbeatTimeout,
			HeartBeatTimeOutMax: c.config.MaxHeartbeatTimeout,
			ElectionTimeOutMin:  c.config.MinElectionTimeout,
			ElectionTimeOutMax:  c.config.MaxElectionTimeout,
			Logger:              c.log,
			Members: func() []gc.ClusterMember {
				if id == 1 { // first node of freshly new cluster
					return []gc.ClusterMember{info}
				}
				return clusterMembers
			}(),
			RpcRequestTimeout: c.config.RpcRequestTimeout,
			PersistenceState:  raftPersistState,
			LogLengthLimit:    c.config.LogLengthLimit,
			SnapshotChunkSize: c.config.SnapshotChunkSize,
			LogFactory:        logFactory,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostURL:              rpcUrl,
			Logger:               c.log,
			HostID:               id,
			RpcRequestTimeout:    c.config.RpcRequestTimeout,
			RpcDialTimeout:       c.config.RpcDialTimeout,
			RpcReconnectDuration: c.config.RpcReconnectDuration,
		},
		EtcdSetup: &node.EtcdSetup{
			HttpServer: http_server.NewEtcdHttpProxyParams{
				URL:    httpUrl,
				Logger: c.log,
			},
			StateMachine: state_machine.NewBtreeKvStateMachineParams{
				Logger:                c.log,
				PersistenceState:      raftPersistState,
				ResponseCacheCapacity: c.config.LogExtensions.Etcd.StateMachineHistoryCapacity,
				BtreeDegree:           c.config.LogExtensions.Etcd.StateMachineBTreeDegree,
				Snapshot:              snapshot,
			},
		},
		LogExtensionEnabled: c.config.LogExtensions.Enable,
		Logger:              c.log,
	}

	n := node.NewNode(ctx, param)
	n.Start(ctx, true, catchingUp)

	c.Nodes[id] = n
	c.createNodeParams[id] = param

	return info, nil
}

func (c *Cluster) initDynamic(filePath string) {
	config, err := gc.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}
	config.Observability.Disabled = true

	c.config = config
	c.MaxElectionTimeout = config.MaxElectionTimeout
	c.MaxHeartbeatTimeout = config.MaxHeartbeatTimeout

	gc.CreateFolderIfNotExists(config.DataFolder)

	log := observability.NewZerolog(config.Observability, 0)
	c.log = log

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	id := 1 // id of first server
	// create first node of cluster
	info, err := c.createNewNode(context.Background(), id)
	if err != nil {
		panic(err)
	}

	c.HttpAgent = go_client.NewHttpClient([]gc.ClusterMember{{ID: info.ID, HttpUrl: info.HttpUrl, RpcUrl: info.RpcUrl}}, c.log)
}
