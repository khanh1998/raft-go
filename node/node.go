package node

import (
	"context"
	"encoding/gob"
	"errors"
	"khanh/raft-go/common"
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/observability"
	"khanh/raft-go/rpc_proxy"
	classicSt "khanh/raft-go/state_machine/classic"
	etcdSt "khanh/raft-go/state_machine/etcd"
)

type HttpServer interface {
	Start()
	Stop()
	SetAccessible()
	SetInaccessible()
}

type Node struct {
	ID     int
	brain  *logic.RaftBrainImpl
	rpc    *rpc_proxy.RPCProxyImpl
	http   HttpServer
	logger observability.Logger
}

type ClassicSetup struct {
	HTTPProxy    http_proxy.NewClassicHttpProxyParams
	StateMachine classicSt.NewClassicStateMachineParams
}

type EtcdSetup struct {
	StateMachine etcdSt.NewBtreeKvStateMachineParams
	HTTPProxy    http_proxy.NewEtcdHttpProxyParams
}

type NewNodeParams struct {
	ID int

	Brain    logic.NewRaftBrainParams
	RPCProxy rpc_proxy.NewRPCImplParams

	// you can only choose one of these two setups
	LogExtensionEnabled common.LogExtension
	ClassicSetup        *ClassicSetup
	EtcdSetup           *EtcdSetup

	Logger observability.Logger
}

func NewEtcdNode(ctx context.Context, params NewNodeParams) *Node {
	stateMachine := etcdSt.NewBtreeKvStateMachine(params.EtcdSetup.StateMachine)
	httpProxy := http_proxy.NewEtcdHttpProxy(params.EtcdSetup.HTTPProxy)

	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRaftBrain", "error", err.Error())
	}

	params.RPCProxy.HostID = params.ID
	rpcProxy, err := rpc_proxy.NewRPCImpl(params.RPCProxy)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRPCImpl", "error", err.Error())
	}

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	brain.SetStateMachine(stateMachine)

	n := &Node{ID: params.ID, brain: brain, rpc: rpcProxy, http: httpProxy, logger: params.Logger}

	return n
}

func NewClassicNode(ctx context.Context, params NewNodeParams) *Node {
	stateMachine := classicSt.NewClassicStateMachine(params.ClassicSetup.StateMachine)
	httpProxy := http_proxy.NewClassicHttpProxy(params.ClassicSetup.HTTPProxy)

	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRaftBrain", "error", err.Error())
	}

	params.RPCProxy.HostID = params.ID
	rpcProxy, err := rpc_proxy.NewRPCImpl(params.RPCProxy)
	if err != nil {
		params.Logger.FatalContext(ctx, "NewNode_NewRPCImpl", "error", err.Error())
	}

	rpcProxy.SetBrain(brain)
	httpProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)

	brain.SetStateMachine(stateMachine)

	n := &Node{ID: params.ID, brain: brain, rpc: rpcProxy, http: httpProxy, logger: params.Logger}

	return n
}

func NewNode(ctx context.Context, params NewNodeParams) *Node {
	sampleLog := params.Brain.LogFactory.Empty()
	gob.Register(sampleLog)

	if params.LogExtensionEnabled == common.LogExtensionClassic {
		return NewClassicNode(ctx, params)
	}

	if params.LogExtensionEnabled == common.LogExtensionEtcd {
		return NewEtcdNode(ctx, params)
	}

	return nil
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
