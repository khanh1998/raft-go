package node

import (
	"context"
	"encoding/gob"
	"errors"
	"khanh/raft-go/common"
	gc "khanh/raft-go/common"
	etcdHttp "khanh/raft-go/extensions/etcd/http_server"
	etcdSt "khanh/raft-go/extensions/etcd/state_machine"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/rpc_proxy"
)

type HttpServer interface {
	Start()
	Stop()
	SetAccessible()
	SetInaccessible()
}

type Node struct {
	ID          int
	ClusterMode gc.ClusterMode
	CatchingUp  bool

	brain  *logic.RaftBrainImpl
	rpc    *rpc_proxy.RPCProxyImpl
	http   HttpServer
	logger observability.Logger
}

type EtcdExtParams struct {
	HttpServer   etcdHttp.NewEtcdHttpProxyParams
	StateMachine etcdSt.NewBtreeKvStateMachineParams
}

type NewNodeParams struct {
	ID        int
	RaftCore  raft_core.NewRaftCoreParams
	Extension *EtcdExtParams

	Logger observability.Logger
}

func NewNode(ctx context.Context, params NewNodeParams) *Node {
	sampleLog := params.RaftCore.Brain.LogFactory.Empty()
	gob.Register(sampleLog)
	stateMachine := etcdSt.NewBtreeKvStateMachine(params.Extension.StateMachine)
	httpProxy := etcdHttp.NewEtcdHttpProxy(params.Extension.HttpServer)

	brain, rpcProxy, err := raft_core.NewRaftCore(ctx, params.RaftCore)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewRaftCore", "error", err)
	}

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	brain.SetStateMachine(stateMachine)

	n := &Node{
		ID: params.ID, ClusterMode: params.RaftCore.Brain.Mode, CatchingUp: params.RaftCore.Brain.CachingUp,
		brain: brain, rpc: rpcProxy, http: httpProxy, logger: params.Logger,
	}

	return n
}

func (n *Node) Start(ctx context.Context) {
	n.SetInaccessible(ctx)
	n.rpc.Start(ctx)
	n.http.Start()

	// when we start a new additional node in dynamic cluster after the first one,
	// it will be in catching-up mode,
	// meaning it will stay still and wait for catching up with leader.
	if !(n.ClusterMode == gc.Dynamic && n.CatchingUp) {
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

func (r *Node) SetNetworkSimulation(network rpc_proxy.NetworkSimulation) {
	r.rpc.SetNetworkSimulation(network)
}

func (r *Node) GetNetworkSimulation() *rpc_proxy.NetworkSimulation {
	return r.rpc.GetNetworkSimulation()
}

func (r *Node) UnsetNetworkSimulation() {
	r.rpc.UnsetNetworkSimulation()
}
