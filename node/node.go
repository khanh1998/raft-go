package node

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"

	"github.com/rs/zerolog/log"
)

type Node struct {
	ID         int
	brain      *logic.RaftBrainImpl
	rpc        *rpc_proxy.RPCProxyImpl
	http       *http_proxy.HttpProxy
	accessible bool
}

type NewNodeParams struct {
	ID        int
	Brain     logic.NewRaftBrainParams
	RPCProxy  rpc_proxy.NewRPCImplParams
	HTTPProxy http_proxy.NewHttpProxyParams
}

func NewNode(params NewNodeParams) *Node {
	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	params.RPCProxy.HostID = params.ID
	rpcProxy, err := rpc_proxy.NewRPCImpl(params.RPCProxy)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("NewNode")
	}

	httpProxy := http_proxy.NewHttpProxy(params.HTTPProxy)

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	n := &Node{ID: params.ID, brain: brain, rpc: rpcProxy, http: httpProxy}

	return n
}

func (n *Node) Start(dynamicCluster bool, cachingUp bool) {
	n.SetInaccessible()
	n.rpc.Start()
	n.http.Start()
	if !(dynamicCluster && cachingUp) {
		n.brain.Start()
	}
	n.SetAccessible()
}

func (n *Node) Stop() error {
	n.rpc.Stop <- struct{}{}
	n.http.Stop <- struct{}{}
	n.brain.Stop <- struct{}{}
	return nil
}

func (n *Node) SetInaccessible() {
	n.accessible = false
	n.http.SetInaccessible()
	n.rpc.Accessible = false
	fmt.Println("the node now is inaccessible", n.http.Accessible)
}

func (n *Node) SetAccessible() {
	n.accessible = true
	n.http.SetAccessible()
	n.rpc.Accessible = true
	fmt.Println("the node now is accessible", n.http.Accessible)
}

func (n *Node) Restart() error {
	return nil
}

func (n *Node) GetStatus() (res common.GetStatusResponse) {
	res = common.GetStatusResponse{
		ID:    n.brain.ID,
		State: n.brain.State,
		Term:  n.brain.CurrentTerm,
	}
	return
}
