package node

import (
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"

	"github.com/rs/zerolog/log"
)

type Node struct {
	brain      *logic.RaftBrainImpl
	rpc        *rpc_proxy.RPCProxyImpl
	http       *http_proxy.HttpProxy
	accessible bool
}

type NewNodeParams struct {
	Brain     logic.NewRaftBrainParams
	RPCProxy  rpc_proxy.NewRPCImplParams
	HTTPProxy http_proxy.NewHttpProxyParams
}

func NewNode(params NewNodeParams) *Node {
	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	rpcProxy, err := rpc_proxy.NewRPCImpl(params.RPCProxy)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("NewNode")
	}

	httpProxy := http_proxy.NewHttpProxy(params.HTTPProxy)

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	rpcProxy.ConnectToPeers(params.RPCProxy.Peers)
	httpProxy.Start()

	brain.Start()

	return &Node{brain: brain, rpc: rpcProxy}
}

type GetStatusResponse struct {
	ID    int
	State logic.RaftState
	Term  int
}

func (n *Node) Stop() error {
	n.rpc.Stop <- struct{}{}
	n.http.Stop <- struct{}{}
	n.brain.Stop <- struct{}{}
	return nil
}

func (n *Node) SetUnaccessible() {
	n.accessible = true
	n.http.Accessile = false
	n.rpc.Accessible = false
}

func (n *Node) SetAccessible() {
	n.accessible = true
	n.http.Accessile = true
	n.rpc.Accessible = true
}

func (n *Node) Restart() error {
	return nil
}

func (n *Node) GetStatus() (res GetStatusResponse) {
	res = GetStatusResponse{
		ID:    n.brain.ID,
		State: n.brain.State,
		Term:  n.brain.CurrentTerm,
	}
	return
}
