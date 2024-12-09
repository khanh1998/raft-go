package testing

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"khanh/raft-go/extensions/etcd/node"
	"khanh/raft-go/observability"
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
			RpcUrl:  params.RaftCore.RPCProxy.HostURL,
			HttpUrl: params.Extension.HttpServer.URL,
		},
	})
}

// for dynamic cluster
func (c *Cluster) createNewNode(ctx context.Context, id int) (info gc.ClusterMember, err error) {
	rpcUrl, httpUrl := fmt.Sprintf("localhost:%d", 1233+id), fmt.Sprintf("localhost:%d", 8079+id)
	info = gc.ClusterMember{ID: id, HttpUrl: httpUrl, RpcUrl: rpcUrl}
	catchingUp := id > 1

	param, err := node.PrepareNewNodeParams(ctx, id, httpUrl, rpcUrl, catchingUp, c.config)
	if err != nil {
		return info, err
	}

	n := node.NewNode(ctx, param)
	n.Start(ctx)

	c.Nodes[id] = n
	c.createNodeParams[id] = param

	return info, nil
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

	id := 1 // id of first server
	// create first node of cluster
	info, err := c.createNewNode(context.Background(), id)
	if err != nil {
		panic(err)
	}

	c.HttpAgent = go_client.NewHttpClient([]gc.ClusterMember{{ID: info.ID, HttpUrl: info.HttpUrl, RpcUrl: info.RpcUrl}}, c.log)
}
