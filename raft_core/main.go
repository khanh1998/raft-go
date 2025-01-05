package raft_core

import (
	"context"
	"fmt"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/common"
	"khanh/raft-go/raft_core/logic"
	"khanh/raft-go/raft_core/rpc_proxy"
)

type NewRaftCoreParams struct {
	ID int

	Brain    logic.NewRaftBrainParams
	RPCProxy rpc_proxy.NewRPCImplParams

	Logger observability.Logger
}

func NewRaftCore(ctx context.Context, params NewRaftCoreParams) (common.RaftBrain, common.InternalRpcServer, error) {
	brain, err := logic.NewRaftBrain(params.Brain)
	if err != nil {
		return nil, nil, fmt.Errorf("NewRaftBrain: %w", err)
	}

	params.RPCProxy.HostID = params.ID
	internalRPC, err := rpc_proxy.NewInternalRPC(params.RPCProxy)
	if err != nil {
		return nil, nil, fmt.Errorf("NewInternalRPC: %w", err)
	}

	internalRPC.SetBrain(brain)
	brain.SetRpcProxy(internalRPC)

	return brain, internalRPC, nil
}
