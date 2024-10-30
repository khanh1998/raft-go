package integration_testing

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/observability"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
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
	return c.HttpAgent.AddServer(id, params.HTTPProxy.URL, params.RPCProxy.HostURL)
}

// dynamic cluster
func (c *Cluster) RemoveServerLeader() error {
	status, err := c.HasOneLeader()
	if err != nil {
		return err
	}

	id := status.ID

	params := c.createNodeParams[id]
	return c.HttpAgent.RemoveServer(id, params.HTTPProxy.URL, params.RPCProxy.HostURL)
}

// dynamic cluster
func (c *Cluster) RemoveServer(id int) error {
	params := c.createNodeParams[id]
	return c.HttpAgent.RemoveServer(id, params.HTTPProxy.URL, params.RPCProxy.HostURL)
}

func (c *Cluster) createNewNode(ctx context.Context, id int) error {
	rpcUrl, httpUrl := fmt.Sprintf("localhost:%d", 1233+id), fmt.Sprintf("localhost:%d", 8079+id)
	catchingUp := id > 1

	dataFolder := fmt.Sprintf("%s%d/", c.config.DataFolder, id)

	smDb, err := common.NewPersistence(dataFolder, "")
	if err != nil {
		return err
	}

	param := node.NewNodeParams{
		ID: id,
		Brain: logic.NewRaftBrainParams{
			ID:                  id,
			Mode:                common.Dynamic,
			CachingUp:           catchingUp,
			HeartBeatTimeOutMin: c.config.MinHeartbeatTimeoutMs,
			HeartBeatTimeOutMax: c.config.MaxHeartbeatTimeoutMs,
			ElectionTimeOutMin:  c.config.MinElectionTimeoutMs,
			ElectionTimeOutMax:  c.config.MaxElectionTimeoutMs,
			Logger:              c.log,
			Members: func() []common.ClusterMember {
				if id == 1 {
					return []common.ClusterMember{{ID: id, RpcUrl: rpcUrl, HttpUrl: httpUrl}}
				}
				return []common.ClusterMember{}
			}(),
			DB:                common.NewPersistenceMock(),
			RpcRequestTimeout: c.config.RpcRequestTimeout,
		},
		RPCProxy: rpc_proxy.NewRPCImplParams{
			HostURL:              rpcUrl,
			Logger:               c.log,
			HostID:               id,
			RpcRequestTimeout:    c.config.RpcRequestTimeout,
			RpcDialTimeout:       c.config.RpcDialTimeout,
			RpcReconnectDuration: c.config.RpcReconnectDuration,
		},
		HTTPProxy: http_proxy.NewHttpProxyParams{
			URL:    httpUrl,
			Logger: c.log,
		},
		StateMachine: state_machine.NewKeyValueStateMachineParams{
			DB:                    smDb,
			DoSnapshot:            c.config.StateMachineSnapshot,
			ClientSessionDuration: uint64(c.config.ClientSessionDuration),
			Logger:                c.log,
		},
		Logger:     c.log,
		DataFolder: dataFolder,
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
	config, err := common.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}
	config.Observability.Disabled = true

	c.config = config
	c.MaxElectionTimeout = time.Duration(config.MaxElectionTimeoutMs * 1000 * 1000)
	c.MaxHeartbeatTimeout = time.Duration(config.MaxHeartbeatTimeoutMs * 1000 * 1000)

	common.CreateFolderIfNotExists(config.DataFolder)

	log := observability.NewZerolog(config.Observability, 0)
	c.log = log

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	id, rpcUrl, httpUrl := 1, "localhost:1234", "localhost:8080"

	// we will use these rpc and http agent to connect to the cluster
	rpcAgent, err := NewRPCImpl(NewRPCImplParams{
		Peers: []common.ClusterMember{{ID: id, HttpUrl: httpUrl, RpcUrl: rpcUrl}},
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
