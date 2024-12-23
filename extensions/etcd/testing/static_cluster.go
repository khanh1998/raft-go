package testing

import (
	"context"
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/extensions/etcd/go_client"
	"khanh/raft-go/extensions/etcd/node"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/rpc_proxy"
	"log"
	"os"
	"sync"
	"time"
)

type Cluster struct {
	Nodes               map[int]*node.Node // nodes or servers in cluster
	HttpAgent           *go_client.HttpClient
	log                 observability.Logger
	createNodeParams    map[int]node.NewNodeParams
	MaxElectionTimeout  time.Duration
	MaxHeartbeatTimeout time.Duration
	config              *common.Config

	httpServerUrls []go_client.Member
	HttpAgents     []*go_client.HttpClient // for concurrency test
}

// after a cluster is created, need to wait a moment so a follower can win a election and become leader
func NewCluster(filePath string) *Cluster {
	c := Cluster{}
	c.init(filePath)

	return &c
}

func (c *Cluster) IsolateNode(nodeId int) error {
	n := c.Nodes[nodeId]
	n.SetInaccessible(context.Background())
	return nil
}

func (c *Cluster) RevertIsolateNode(nodeId int) error {
	n := c.Nodes[nodeId]
	n.SetAccessible(context.Background())
	return nil
}

func (c *Cluster) InitConcurrencyTest(client int) error {
	for i := 0; i < client; i++ {
		httpAgent, err := go_client.NewHttpClient(c.httpServerUrls, c.log)
		if err != nil {
			return err
		}
		c.HttpAgents = append(c.HttpAgents, httpAgent)
	}

	return nil
}

func (c *Cluster) init(filePath string) {
	ctx := context.Background()
	config, err := common.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}
	config.Observability.Disabled = true

	c.config = config

	c.MaxElectionTimeout = config.RaftCore.MaxElectionTimeout
	c.MaxHeartbeatTimeout = config.RaftCore.MaxHeartbeatTimeout

	// check data folder
	gc.CreateFolderIfNotExists(config.RaftCore.DataFolder)

	log := observability.NewZerolog(config.Observability, 0)

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	lock := sync.Mutex{}

	l := sync.WaitGroup{}
	for _, mem := range config.RaftCore.Cluster.Servers {
		log := log.With("ID", mem.ID)
		l.Add(1)
		go func(mem gc.ClusterServerConfig) {

			id := mem.ID
			rpcUrl := fmt.Sprintf("%s:%d", mem.Host, mem.RpcPort)
			httpUrl := fmt.Sprintf("%s:%d", mem.Host, mem.HttpPort)

			param, err := node.PrepareNewNodeParams(ctx, id, httpUrl, rpcUrl, false, c.config, c.log)
			if err != nil {
				log.FatalContext(ctx, "PrepareNewNodeParams", "error", err)
			}

			n := node.NewNode(ctx, param)
			n.Start(ctx)

			nsCfg := c.config.NetworkSimulation
			if nsCfg.Enable {
				n.SetNetworkSimulation(rpc_proxy.NetworkSimulation{
					MinDelay:    nsCfg.MinDelay,
					MaxDelay:    nsCfg.MaxDelay,
					MsgDropRate: nsCfg.MsgDropRate,
					Logger:      log,
				})
			}

			lock.Lock()
			c.createNodeParams[mem.ID] = param
			c.Nodes[mem.ID] = n
			l.Done()
			lock.Unlock()
		}(mem)
	}

	httpServerUrls := []go_client.Member{}

	for _, server := range config.RaftCore.Cluster.Servers {
		httpServerUrls = append(httpServerUrls, go_client.Member{
			ID:     server.ID,
			Host:   fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
			Scheme: "http", // http for local testing
		})
	}

	httpAgent, err := go_client.NewHttpClient(httpServerUrls, log)
	if err != nil {
		log.FatalContext(ctx, "StaticCluster_NewHttpClient", "error", err)
	}

	c.httpServerUrls = httpServerUrls
	c.HttpAgent = httpAgent
	c.log = log

	l.Wait()
}

var (
	ErrMoreThanOneLeader = errors.New("there are more than one leader in the cluster")
	ErrWrongExpectedTerm = errors.New("wrong expected term")
	ErrThereIsNoLeader   = errors.New("there is no leader")
)

func (c *Cluster) DisconnectLeader() error {

	return nil
}

func (c *Cluster) StopFollower() (nodeId int, err error) {
	status, err := c.FindFirstFollower()
	if err != nil {
		return 0, err
	}

	return status.ID, c.StopNode(status.ID)
}

func (c *Cluster) StopLeader() (nodeId int, err error) {
	status, err := c.HasOneLeader()
	if err != nil {
		return 0, err
	}

	return status.ID, c.StopNode(status.ID)
}

func (c *Cluster) StartAll() {
	for _, node := range c.Nodes {
		err := c.StartNode(node.ID)
		if err != nil {
			c.log.Error("StartAll_StartNode", err)
		}
	}
}

func (c *Cluster) StopAll() {
	for _, node := range c.Nodes {
		err := c.StopNode(node.ID)
		if err != nil {
			c.log.Error("StopAll_StopNode", err)
		}
	}
}

func (c *Cluster) StopNode(nodeId int) error {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		if node.ID == nodeId {
			node.Stop(context.Background())
		}
	}
	return nil
}

// for static cluster only
func (c *Cluster) StartNode(nodeId int) error {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		if node.ID == nodeId {
			node.Start(context.Background())
			break
		}
	}

	return nil
}

func (c *Cluster) RestartNode(nodeId int, sleep time.Duration) {
	for index, n := range c.Nodes {
		if n == nil {
			continue
		}
		if n.ID == nodeId {
			c.Nodes[index].Stop(context.Background())
			time.AfterFunc(sleep, func() {
				// new node will read data from log file and recreate the state of the node
				c.Nodes[index] = node.NewNode(context.Background(), c.createNodeParams[index])
				c.log.Info(fmt.Sprintf("new node is replaced %d", nodeId))
			})
			break
		}
	}
}

func (c *Cluster) CountLiveNode() (count int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	for _, n := range c.Nodes {
		_, err = c.HttpAgent.GetInfo(ctx, n.ID)
		if err != nil {
			continue
		}

		count += 1
	}
	return count, nil
}

func (c *Cluster) FindFirstFollower() (status gc.GetStatusResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err = c.HttpAgent.GetInfo(ctx, n.ID)
		if err != nil {
			c.log.Error("HasOneLeader_GetInfo", err)

			continue
		}

		if status.State == gc.StateFollower {
			return
		}
	}

	return status, errors.New("can't found any follower")
}
func (c *Cluster) HasOneLeader() (gc.GetStatusResponse, error) {
	leaderCount := 0
	var leaderStatus gc.GetStatusResponse
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err := c.HttpAgent.GetInfo(ctx, n.ID)
		if err != nil {
			c.log.Error("HasOneLeader_GetInfo", err)

			continue
		}

		c.log.Info("HasOneLeader", "status", status)

		if status.State == gc.StateLeader {
			if leaderCount == 0 {
				leaderStatus = status
			}
			leaderCount += 1
		}
	}

	if leaderCount > 1 {
		return leaderStatus, ErrMoreThanOneLeader
	} else if leaderCount == 0 {
		return leaderStatus, ErrThereIsNoLeader
	}

	return leaderStatus, nil
}

func (c Cluster) Clean() {
	for id, node := range c.Nodes {
		node.Stop(context.Background())
		os.RemoveAll(fmt.Sprintf("data/%d", id))
		delete(c.Nodes, id)
	}
}
