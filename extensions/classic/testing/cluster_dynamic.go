package integration_testing

import (
	"context"
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/classic/common"
	"khanh/raft-go/extensions/classic/node"
	"khanh/raft-go/observability"
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
	params, ok := c.createNodeParams[id]
	if ok {
		return c.HttpAgent.AddServer(id, params.Extension.HttpServer.URL, params.RaftCore.RPCProxy.HostURL)
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

	params, ok := c.createNodeParams[id]
	if ok {
		return c.HttpAgent.RemoveServer(id, params.Extension.HttpServer.URL, params.RaftCore.RPCProxy.HostURL)
	}

	return errors.New("no setup was found")
}

// dynamic cluster
func (c *Cluster) RemoveServer(id int) error {
	params, ok := c.createNodeParams[id]
	if ok {
		return c.HttpAgent.RemoveServer(id, params.Extension.HttpServer.URL, params.RaftCore.RPCProxy.HostURL)
	}

	return errors.New("no setup was found")
}

// create or recreate
func (c *Cluster) createNewNode(ctx context.Context, id int) error {
	rpcUrl, httpUrl := fmt.Sprintf("localhost:%d", 1233+id), fmt.Sprintf("localhost:%d", 8079+id)
	catchingUp := id > 1

	param, err := node.PrepareNewNodeParams(ctx, id, httpUrl, rpcUrl, catchingUp, c.config, c.log)
	if err != nil {
		return err
	}

	n := node.NewNode(ctx, param)
	n.Start(ctx)

	c.createNodeParams[id] = param
	c.Nodes[id] = n

	// create new
	if _, ok := c.HttpAgent.serverUrls[id]; !ok {
		c.HttpAgent.serverInfos = append(c.HttpAgent.serverInfos, HttpServerConnectionInfo{
			Id:  id,
			Url: httpUrl,
		})
		c.HttpAgent.serverUrls[id] = httpUrl
	}
	return c.RpcAgent.ConnectToRpcServer(id, rpcUrl, 5, 150*time.Millisecond)
}

func (c *Cluster) initDynamic(filePath string) {
	config, err := common.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}
	config.Observability.Disabled = true

	c.config = config
	c.MaxElectionTimeout = config.RaftCore.MaxElectionTimeout
	c.MaxHeartbeatTimeout = config.RaftCore.MaxHeartbeatTimeout

	gc.CreateFolderIfNotExists(config.RaftCore.DataFolder)

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
