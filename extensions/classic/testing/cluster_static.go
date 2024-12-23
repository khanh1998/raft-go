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
	"os"
	"sync"
	"time"
)

const (
	NodeRunning = "running"
	NodeStopped = "stopped"
)

type Cluster struct {
	Nodes               map[int]*node.Node // nodes or servers in cluster
	NodeState           map[int]string
	RpcAgent            RpcAgentImpl // use this rpc agent to get real-time status from servers in cluster
	HttpAgent           HttpAgent
	log                 observability.Logger
	createNodeParams    map[int]node.NewNodeParams
	MaxElectionTimeout  time.Duration
	MaxHeartbeatTimeout time.Duration
	config              *common.Config
	lock                sync.Mutex
}

// after a cluster is created, need to wait a moment so a follower can win a election and become leader
func NewCluster(filePath string) *Cluster {
	c := Cluster{}
	c.init(filePath)

	return &c
}

func (c *Cluster) createStaticNode(ctx context.Context, mem gc.ClusterServerConfig) {
	id := mem.ID
	rpcUrl := fmt.Sprintf("%s:%d", mem.Host, mem.RpcPort)
	httpUrl := fmt.Sprintf("%s:%d", mem.Host, mem.HttpPort)

	param, err := node.PrepareNewNodeParams(ctx, id, httpUrl, rpcUrl, false, c.config, c.log)
	if err != nil {
		c.log.FatalContext(ctx, "PrepareNewNodeParams", "error", err)
	}

	n := node.NewNode(ctx, param)
	n.Start(ctx)

	c.lock.Lock()
	c.createNodeParams[mem.ID] = param
	c.Nodes[mem.ID] = n
	c.lock.Unlock()
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

	gc.CreateFolderIfNotExists(config.RaftCore.DataFolder)

	log := observability.NewZerolog(config.Observability, 0)
	c.log = log

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	l := sync.WaitGroup{}
	for _, mem := range config.RaftCore.Cluster.Servers {
		l.Add(1)
		go func(mem gc.ClusterServerConfig) {
			c.createStaticNode(ctx, mem)
			l.Done()
		}(mem)
	}

	httpServerUrls := []HttpServerConnectionInfo{}
	peers := []gc.ClusterMember{}

	for _, server := range config.RaftCore.Cluster.Servers {
		httpServerUrls = append(httpServerUrls, HttpServerConnectionInfo{
			Id:  server.ID,
			Url: fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
		})

		peers = append(peers, gc.ClusterMember{
			ID:      server.ID,
			RpcUrl:  fmt.Sprintf("%s:%d", server.Host, server.RpcPort),
			HttpUrl: fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
		})
	}

	rpcAgent, err := NewRPCImpl(NewRPCImplParams{Peers: peers, Log: log})
	if err != nil {
		panic(err)
	}

	httpAgent := NewHttpAgent(HttpAgentArgs{
		serverUrls: httpServerUrls,
		Log:        log,
	})

	c.RpcAgent = *rpcAgent
	c.HttpAgent = *httpAgent

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
			err := c.RpcAgent.disconnect(nodeId)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

// for static cluster only.
// to start a stopped node.
func (c *Cluster) StartNode(nodeId int) error {
	for _, memInfo := range c.config.RaftCore.Cluster.Servers {
		if memInfo.ID == nodeId {
			// this command helps us to deserialize data from files, it also starts the node.
			c.createStaticNode(context.Background(), memInfo)
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
				err := c.createNewNode(context.Background(), nodeId)
				if err != nil {
					c.log.Error("RestartNode", err)
				} else {
					c.log.Info(fmt.Sprintf("new node is replaced %d", nodeId))
				}
			})
			break
		}
	}
}

func (c *Cluster) CountLiveNode() (count int, err error) {
	timeout := 50 * time.Millisecond
	for _, n := range c.Nodes {
		_, err = c.RpcAgent.SendPing(n.ID, &timeout)
		if err != nil {
			continue
		}

		count += 1
	}
	return count, nil
}

func (c *Cluster) FindFirstFollower() (status gc.GetStatusResponse, err error) {
	timeout := 100 * time.Millisecond
	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err = c.RpcAgent.GetInfo(n.ID, &timeout)
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
	timeout := 100 * time.Millisecond

	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err := c.RpcAgent.GetInfo(n.ID, &timeout)
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
