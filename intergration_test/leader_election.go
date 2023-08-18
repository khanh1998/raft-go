package intergration_test

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/node"
	"khanh/raft-go/rpc_proxy"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Cluster struct {
	Nodes []*node.Node
}

func NewCluster(numNode int) *Cluster {
	c := Cluster{}
	c.init(numNode)

	return &c
}

func (c *Cluster) init(num int) {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	log := zerolog.New(output).With().Timestamp().Logger()

	id := 1
	rpcPort := 1234
	httpPort := 8080

	peers := []common.PeerInfo{}

	for i := 0; i < num; i++ {
		peers = append(peers, common.PeerInfo{
			ID:  id + i,
			URL: fmt.Sprintf(":%d", rpcPort+i),
		})
	}

	c.Nodes = make([]*node.Node, num)

	l := sync.WaitGroup{}
	for i := 0; i < num; i++ {
		l.Add(1)
		go func(i int) {
			param := node.NewNodeParams{
				ID: id + 1,
				Brain: logic.NewRaftBrainParams{
					DataFileName:      fmt.Sprintf("test.log.%d.dat", id+i),
					MinRandomDuration: 150,
					MaxRandomDuration: 300,
					Log:               &log,
					Peers:             peers,
				},
				RPCProxy: rpc_proxy.NewRPCImplParams{
					Peers:   peers,
					HostURL: fmt.Sprintf(":%d", rpcPort+i),
					Log:     &log,
				},
				HTTPProxy: http_proxy.NewHttpProxyParams{
					URL: fmt.Sprintf("localhost:%d", httpPort+i),
				},
			}

			n := node.NewNode(param)

			c.Nodes[i] = n
			l.Done()
		}(i)
	}

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
		if node.ID == nodeId {
			node.Stop()
			break
		}
	}
}

func (c *Cluster) HasOneLeader() (node.GetStatusResponse, error) {
	leaderCount := 0
	var leaderStatus node.GetStatusResponse
	for _, n := range c.Nodes {
		status := n.GetStatus()
		if status.State == logic.StateLeader {
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
