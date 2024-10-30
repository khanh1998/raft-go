package node

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/observability"
	"khanh/raft-go/rpc_proxy"
	"khanh/raft-go/state_machine"
)

type Node struct {
	ID     int
	brain  *logic.RaftBrainImpl
	rpc    *rpc_proxy.RPCProxyImpl
	http   *http_proxy.HttpProxy
	logger observability.Logger
}

type NewNodeParams struct {
	ID int

	Brain        logic.NewRaftBrainParams
	RPCProxy     rpc_proxy.NewRPCImplParams
	HTTPProxy    http_proxy.NewHttpProxyParams
	StateMachine state_machine.NewKeyValueStateMachineParams

	Logger     observability.Logger
	DataFolder string
}

func NewNode(ctx context.Context, params NewNodeParams) *Node {
	stateMachine, err := state_machine.NewKeyValueStateMachine(params.StateMachine)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewKeyValueStateMachine", "error", err)
	}

	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRaftBrain", "error", err.Error())
	}

	params.RPCProxy.HostID = params.ID
	rpcProxy, err := rpc_proxy.NewRPCImpl(params.RPCProxy)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRPCImpl", "error", err.Error())
	}

	httpProxy := http_proxy.NewHttpProxy(params.HTTPProxy)

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	stateMachine.SetConsensusModule(brain)
	brain.SetStateMachine(stateMachine)

	n := &Node{ID: params.ID, brain: brain, rpc: rpcProxy, http: httpProxy, logger: params.Logger}

	return n
}

func (n *Node) Start(ctx context.Context, dynamicCluster bool, cachingUp bool) {
	n.SetInaccessible(ctx)
	n.rpc.Start(ctx)
	n.http.Start()
	if !(dynamicCluster && cachingUp) {
		n.brain.Start(ctx)
	}
	n.SetAccessible(ctx)
}

func (n *Node) Crash() error {
	return errors.New("not implemented")
}

func (n *Node) Stop(ctx context.Context) error {
	n.SetInaccessible(ctx)
	n.rpc.Stop()
	n.http.Stop()
	n.brain.Stop()
	return nil
}

func (n *Node) SetInaccessible(ctx context.Context) {
	n.http.SetInaccessible()
	n.rpc.SetInaccessible()
	n.log().InfoContext(ctx, "the node now is inaccessible")
}

func (n *Node) SetAccessible(ctx context.Context) {
	n.http.SetAccessible()
	n.rpc.SetAccessible()
	n.log().InfoContext(ctx, "the node now is accessible")
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

func (n *Node) log() observability.Logger {
	// data race
	sub := n.logger.With(
		"id", n.ID,
	)
	return sub
}
