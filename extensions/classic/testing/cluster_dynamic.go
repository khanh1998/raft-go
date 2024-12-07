package integration_testing

import (
	"context"
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/classic/common"
	http_proxy "khanh/raft-go/extensions/classic/http_server"
	"khanh/raft-go/extensions/classic/state_machine"
	"khanh/raft-go/node"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
	"log"
	"time"
)

// after a cluster is created, need to wait a moment so a follower can win a election and become leader
func NewDynamicCluster(filePath string) *Cluster {
	c := Cluster{}
	c.initDynamic(filePath)

	return &c
}

// dynamic cluster
func (c *Cluster) AddServer(id int) error {
	params := c.createNodeParams[id]
	if params.ClassicSetup != nil {
		return c.HttpAgent.AddServer(id, params.ClassicSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}
	if params.EtcdSetup != nil {
		return c.HttpAgent.AddServer(id, params.EtcdSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}
	return errors.New("no setup was found")
}

// dynamic cluster
func (c *Cluster) RemoveServerLeader() error {
	status, err := c.HasOneLeader()
	if err != nil {
		return err
	}

	id := status.ID

	params := c.createNodeParams[id]
	if params.ClassicSetup != nil {
		return c.HttpAgent.RemoveServer(id, params.ClassicSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}

	if params.EtcdSetup != nil {
		return c.HttpAgent.RemoveServer(id, params.EtcdSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}

	return errors.New("no setup was found")
}

// dynamic cluster
func (c *Cluster) RemoveServer(id int) error {
	params := c.createNodeParams[id]
	if params.ClassicSetup != nil {
		return c.HttpAgent.RemoveServer(id, params.ClassicSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}

	if params.EtcdSetup != nil {
		return c.HttpAgent.RemoveServer(id, params.EtcdSetup.HttpServer.URL, params.RPCProxy.HostURL)
	}
	return errors.New("no setup was found")
}

func (c *Cluster) createNewNode(ctx context.Context, id int) error {
	rpcUrl, httpUrl := fmt.Sprintf("localhost:%d", 1233+id), fmt.Sprintf("localhost:%d", 8079+id)
	catchingUp := id > 1

	dataFolder := fmt.Sprintf("%s%d/", c.config.DataFolder, id)

	storage, err := storage.NewStorage(storage.NewStorageParams{
		WalSize:    c.config.WalSizeLimit,
		DataFolder: dataFolder,
		Logger:     c.log,
	}, storage.FileWrapperImpl{})
	if err != nil {
		return err
	}

	logFactory := common.ClassicLogFactory{
		NewSnapshot: state_machine.NewClassicSnapshotI,
	}

	snapshot, raftPersistState, clusterMembers, err := persistence_state.Deserialize(ctx, storage, gc.Dynamic, c.log, logFactory)
	if err != nil {
		return err
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
					return []gc.ClusterMember{{ID: id, RpcUrl: rpcUrl, HttpUrl: httpUrl}}
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
		ClassicSetup: &node.ClassicSetup{
			HttpServer: http_proxy.NewClassicHttpProxyParams{
				URL:    httpUrl,
				Logger: c.log,
			},
			StateMachine: state_machine.NewClassicStateMachineParams{
				ClientSessionDuration: uint64(c.config.LogExtensions.Classic.ClientSessionDuration),
				Logger:                c.log,
				PersistState:          raftPersistState,
				Snapshot:              snapshot,
			},
		},
		LogExtensionEnabled: gc.LogExtensionClassic,
		Logger:              c.log,
	}

	n := node.NewNode(ctx, param)
	n.Start(ctx, true, catchingUp)

	c.createNodeParams[id] = param
	c.Nodes[id] = n
	c.HttpAgent.serverInfos = append(c.HttpAgent.serverInfos, HttpServerConnectionInfo{
		Id:  id,
		Url: httpUrl,
	})
	c.HttpAgent.serverUrls[id] = httpUrl
	return c.RpcAgent.ConnectToRpcServer(id, rpcUrl, 5, 150*time.Millisecond)
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

	id, rpcUrl, httpUrl := 1, "localhost:1234", "localhost:8080"

	// we will use these rpc and http agent to connect to the cluster
	rpcAgent, err := NewRPCImpl(NewRPCImplParams{
		Peers: []gc.ClusterMember{{ID: id, HttpUrl: httpUrl, RpcUrl: rpcUrl}},
		Log:   log,
	})
	if err != nil {
		panic(err)
	}

	httpServerUrls := []HttpServerConnectionInfo{
		{
			Id:  id,
			Url: httpUrl,
		},
	}

	httpAgent := NewHttpAgent(HttpAgentArgs{
		serverUrls: httpServerUrls,
		Log:        log,
	})

	c.RpcAgent = *rpcAgent
	c.HttpAgent = *httpAgent
	c.HttpAgent.leaderId = 1 // initially, the leader is the first node, which is 1

	// create first node of cluster
	err = c.createNewNode(context.Background(), id)
	if err != nil {
		panic(err)
	}

}
