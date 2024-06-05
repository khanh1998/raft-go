package node

import (
	"errors"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Node struct {
	ID     int
	brain  *logic.RaftBrainImpl
	rpc    *rpc_proxy.RPCProxyImpl
	http   *http_proxy.HttpProxy
	logger *zerolog.Logger
}

type NewNodeParams struct {
	ID        int
	Brain     logic.NewRaftBrainParams
	RPCProxy  rpc_proxy.NewRPCImplParams
	HTTPProxy http_proxy.NewHttpProxyParams
	Logger    *zerolog.Logger
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

	n := &Node{ID: params.ID, brain: brain, rpc: rpcProxy, http: httpProxy, logger: params.Logger}

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

func (n *Node) Crash() error {
	return errors.New("not implemented")
}

func (n *Node) Stop() error {
	n.SetInaccessible()
	n.rpc.Stop()
	n.http.Stop()
	n.brain.Stop()
	return nil
}

func (n *Node) SetInaccessible() {
	n.http.SetInaccessible()
	n.rpc.SetInaccessible()
	n.log().Info().Msg("the node now is inaccessible")
}

func (n *Node) SetAccessible() {
	n.http.SetAccessible()
	n.rpc.SetAccessible()
	n.log().Info().Msg("the node now is accessible")
}

func (n *Node) Restart() error {
	return nil
}

func (n *Node) GetStatus() (res common.GetStatusResponse) {
	res = common.GetStatusResponse{
		ID:    n.brain.GetId(),
		State: n.brain.GetState(),
		Term:  n.brain.GetCurrentTerm(),
	}
	return
}

func (n *Node) log() *zerolog.Logger {
	// data race
	sub := n.logger.With().
		Int("id", n.ID).
		Logger()
	return &sub
}
