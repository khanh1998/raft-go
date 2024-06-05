package integration_testing

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"log"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Cluster struct {
	Nodes               map[int]*node.Node // nodes or servers in cluster
	RpcAgent            RpcAgentImpl       // use this rpc agent to get real-time status from servers in cluster
	HttpAgent           HttpAgent
	log                 *zerolog.Logger
	createNodeParams    map[int]node.NewNodeParams
	MaxElectionTimeout  time.Duration
	MaxHeartbeatTimeout time.Duration
	config              *common.Config
}

func NewCluster(filePath string) *Cluster {
	c := Cluster{}
	c.init(filePath)

	return &c
}

func (c *Cluster) init(filePath string) {
	config, err := common.ReadConfigFromFile(&filePath)
	if err != nil {
		log.Panic(err)
	}

	c.config = config

	c.MaxElectionTimeout = time.Duration(config.MaxElectionTimeoutMs * 1000 * 1000)
	c.MaxHeartbeatTimeout = time.Duration(config.MaxHeartbeatTimeoutMs * 1000 * 1000)

	zerolog.TimeFieldFormat = time.RFC3339Nano
	stdOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	log := zerolog.New(stdOutput).With().Timestamp().Logger()

	peers := []common.ClusterMember{}
	for _, mem := range config.Cluster.Servers {
		peers = append(peers, common.ClusterMember{
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
		go func(mem common.ClusterMember) {
			param := node.NewNodeParams{
				ID: mem.ID,
				Brain: logic.NewRaftBrainParams{
					ID:                  mem.ID,
					Mode:                common.Static,
					CachingUp:           false,
					DataFileName:        fmt.Sprintf("test.log.%d.dat", mem.ID),
					HeartBeatTimeOutMin: config.MinHeartbeatTimeoutMs,
					HeartBeatTimeOutMax: config.MaxHeartbeatTimeoutMs,
					ElectionTimeOutMin:  config.MinElectionTimeoutMs,
					ElectionTimeOutMax:  config.MaxElectionTimeoutMs,
					Logger:              &log,
					Members:             peers,
					// DB:                persistance.NewPersistence(fmt.Sprintf("test.log.%d.dat", id+i)),
					DB:           persistance.NewPersistenceMock(),
					StateMachine: common.NewKeyValueStateMachine(),
				},
				RPCProxy: rpc_proxy.NewRPCImplParams{
					HostURL: mem.RpcUrl,
					Logger:  &log,
					HostID:  mem.ID,
				},
				HTTPProxy: http_proxy.NewHttpProxyParams{
					URL:    mem.HttpUrl,
					Logger: &log,
				},
				Logger: &log,
			}

			n := node.NewNode(param)
			n.Start(false, false)

			c.createNodeParams[mem.ID] = param
			c.Nodes[mem.ID] = n
			l.Done()
		}(mem)
	}

	rpcAgent, err := NewRPCImpl(NewRPCImplParams{Peers: peers, Log: &log})
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
		Log:        &log,
	})

	c.RpcAgent = *rpcAgent
	c.HttpAgent = *httpAgent
	c.log = &log

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

func (c *Cluster) StopLeader() (nodeId int, err error) {
	status, err := c.HasOneLeader()
	if err != nil {
		return 0, err
	}

	return status.ID, c.StopNode(status.ID)
}

func (c *Cluster) StopAll() {
	for _, node := range c.Nodes {
		node.Stop()
	}
}

func (c *Cluster) StopNode(nodeId int) error {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		if node.ID == nodeId {
			node.Stop()
			err := c.RpcAgent.Disconnect(nodeId)
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
			node.Start(false, false)
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
			c.Nodes[index].Stop()
			time.AfterFunc(sleep, func() {
				// new node will read data from log file and recreate the state of the node
				c.Nodes[index] = node.NewNode(c.createNodeParams[index])
				c.log.Info().Msgf("new node is replaced %d", nodeId)
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

func (c *Cluster) HasOneLeader() (common.GetStatusResponse, error) {
	leaderCount := 0
	var leaderStatus common.GetStatusResponse
	timeout := 100 * time.Millisecond

	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err := c.RpcAgent.GetInfo(n.ID, &timeout)
		if err != nil {
			c.log.Err(err).Msg("HasOneLeader_GetInfo")

			continue
		}

		c.log.Info().Interface("status", status).Msgf("status node")

		if status.State == common.StateLeader {
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
		node.Stop()
		os.Remove(fmt.Sprintf("test.log.%d.dat", id))
		delete(c.Nodes, id)
	}
}
