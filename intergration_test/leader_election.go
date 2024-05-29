package intergration_test

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/persistance"
	"khanh/raft-go/rpc_proxy"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Cluster struct {
	Nodes            []*node.Node
	RpcProxy         RPCProxyImpl
	log              *zerolog.Logger
	createNodeParams []node.NewNodeParams
}

func NewCluster(numNode int) *Cluster {
	c := Cluster{}
	c.init(numNode)

	return &c
}

func (c *Cluster) init(num int) {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}

	log := zerolog.New(output).With().Timestamp().Logger()

	id := 1
	rpcPort := 1234
	httpPort := 8080

	peers := []common.ClusterMember{}

	for i := 0; i < num; i++ {
		peers = append(peers, common.ClusterMember{
			ID:     id + i,
			RpcUrl: fmt.Sprintf(":%d", rpcPort+i),
		})
	}

	c.Nodes = make([]*node.Node, num)
	c.createNodeParams = make([]node.NewNodeParams, num)

	l := sync.WaitGroup{}
	for i := 0; i < num; i++ {
		l.Add(1)
		go func(i int) {
			param := node.NewNodeParams{
				ID: id + i,
				Brain: logic.NewRaftBrainParams{
					ID:                  0,
					Mode:                common.Static,
					CachingUp:           false,
					DataFileName:        fmt.Sprintf("test.log.%d.dat", id+i),
					HeartBeatTimeOutMin: 150,
					HeartBeatTimeOutMax: 300,
					ElectionTimeOutMin:  300,
					ElectionTimeOutMax:  500,
					Log:                 &log,
					Members:             []common.ClusterMember{},
					// DB:                persistance.NewPersistence(fmt.Sprintf("test.log.%d.dat", id+i)),
					DB:           persistance.NewPersistenceMock(),
					StateMachine: common.NewKeyValueStateMachine(),
				},
				RPCProxy: rpc_proxy.NewRPCImplParams{
					HostURL: fmt.Sprintf(":%d", rpcPort+i),
					Log:     &log,
				},
				HTTPProxy: http_proxy.NewHttpProxyParams{
					URL: fmt.Sprintf("localhost:%d", httpPort+i),
				},
			}

			n := node.NewNode(param)
			n.Start(param.Brain.Mode == common.Dynamic, param.Brain.CachingUp)

			c.createNodeParams[i] = param
			c.Nodes[i] = n
			l.Done()
		}(i)
	}

	rpcProxy, err := NewRPCImpl(NewRPCImplParams{Peers: peers, Log: &log})
	if err != nil {
		panic(err)
	}

	c.RpcProxy = *rpcProxy
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

func (c *Cluster) StopNode(nodeId int) {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		if node.ID == nodeId {
			node.Stop()
			break
		}
	}
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

func (c *Cluster) HasOneLeader() (common.GetStatusResponse, error) {
	leaderCount := 0
	var leaderStatus common.GetStatusResponse
	timeout := 100 * time.Millisecond

	for _, n := range c.Nodes {
		if n == nil {
			continue
		}
		status, err := c.RpcProxy.GetInfo(n.ID, &timeout)
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
	for i := 0; i < len(c.Nodes); i++ {
		os.Remove(fmt.Sprintf("test.log.%d.dat", i+1))
	}
}
