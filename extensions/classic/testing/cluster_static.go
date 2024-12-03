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
	"os"
	"sync"
	"time"
)

type Cluster struct {
	Nodes               map[int]*node.Node // nodes or servers in cluster
	RpcAgent            RpcAgentImpl       // use this rpc agent to get real-time status from servers in cluster
	HttpAgent           HttpAgent
	log                 observability.Logger
	createNodeParams    map[int]node.NewNodeParams
	MaxElectionTimeout  time.Duration
	MaxHeartbeatTimeout time.Duration
	config              *gc.Config
}

// after a cluster is created, need to wait a moment so a follower can win a election and become leader
func NewCluster(filePath string) *Cluster {
	c := Cluster{}
	c.init(filePath)

	return &c
}

func (c *Cluster) init(filePath string) {
	ctx := context.Background()
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

	peers := []gc.ClusterMember{}
	for _, mem := range config.Cluster.Servers {
		peers = append(peers, gc.ClusterMember{
			ID:      mem.ID,
			RpcUrl:  fmt.Sprintf("%s:%d", mem.Host, mem.RpcPort),
			HttpUrl: fmt.Sprintf("%s:%d", mem.Host, mem.HttpPort),
		})
	}

	c.Nodes = make(map[int]*node.Node)
	c.createNodeParams = make(map[int]node.NewNodeParams)

	l := sync.WaitGroup{}
	for _, mem := range peers {
		l.Add(1)
		go func(mem gc.ClusterMember) {
			dataFolder := fmt.Sprintf("%s%d/", c.config.DataFolder, mem.ID)

			storage, err := storage.NewStorage(storage.NewStorageParams{
				WalSize:    config.WalSizeLimit,
				DataFolder: dataFolder,
				Logger:     log,
			}, storage.FileWrapperImpl{})
			if err != nil {
				log.Fatal("new storage", "error", err.Error())
			}

			logFactory := common.ClassicLogFactory{
				NewSnapshot: state_machine.NewClassicSnapshotI,
			}

			snapshot, raftPersistState, _, err := persistence_state.Deserialize(ctx, storage, gc.Static, log, logFactory)
			if err != nil {
				log.Fatal("deserialize system", "error", err.Error())
			}

			raftPersistState.SetStorage(storage)

			param := node.NewNodeParams{
				ID: mem.ID,
				Brain: logic.NewRaftBrainParams{
					ID:                  mem.ID,
					Mode:                gc.Static,
					CachingUp:           false,
					HeartBeatTimeOutMin: config.MinHeartbeatTimeout,
					HeartBeatTimeOutMax: config.MaxHeartbeatTimeout,
					ElectionTimeOutMin:  config.MinElectionTimeout,
					ElectionTimeOutMax:  config.MaxElectionTimeout,
					Logger:              log,
					Members:             peers,
					RpcRequestTimeout:   config.RpcRequestTimeout,
					PersistenceState:    raftPersistState,
					LogLengthLimit:      config.LogLengthLimit,
					SnapshotChunkSize:   config.SnapshotChunkSize,
					LogFactory:          logFactory,
				},
				RPCProxy: rpc_proxy.NewRPCImplParams{
					HostURL:              mem.RpcUrl,
					Logger:               log,
					HostID:               mem.ID,
					RpcRequestTimeout:    config.RpcRequestTimeout,
					RpcDialTimeout:       config.RpcDialTimeout,
					RpcReconnectDuration: config.RpcReconnectDuration,
				},
				ClassicSetup: &node.ClassicSetup{
					HttpServer: http_proxy.NewClassicHttpProxyParams{
						URL:    mem.HttpUrl,
						Logger: log,
					},
					StateMachine: state_machine.NewClassicStateMachineParams{
						ClientSessionDuration: uint64(config.LogExtensions.Classic.ClientSessionDuration),
						Logger:                log,
						PersistState:          raftPersistState,
						Snapshot:              snapshot,
					},
				},
				LogExtensionEnabled: gc.LogExtensionClassic,
				Logger:              log,
			}

			n := node.NewNode(ctx, param)
			n.Start(ctx, false, false)

			c.createNodeParams[mem.ID] = param
			c.Nodes[mem.ID] = n
			l.Done()
		}(mem)
	}

	rpcAgent, err := NewRPCImpl(NewRPCImplParams{Peers: peers, Log: log})
	if err != nil {
		panic(err)
	}

	httpServerUrls := []HttpServerConnectionInfo{}

	for _, server := range config.Cluster.Servers {
		httpServerUrls = append(httpServerUrls, HttpServerConnectionInfo{
			Id:  server.ID,
			Url: fmt.Sprintf("%s:%d", server.Host, server.HttpPort),
		})
	}

	httpAgent := NewHttpAgent(HttpAgentArgs{
		serverUrls: httpServerUrls,
		Log:        log,
	})

	c.RpcAgent = *rpcAgent
	c.HttpAgent = *httpAgent
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
			err := c.RpcAgent.disconnect(nodeId)
			if err != nil {
				return err
			}
			break
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
			node.Start(context.Background(), false, false)
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
