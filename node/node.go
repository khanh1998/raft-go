package node

import (
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"
)

type Node struct {
}

func NewNode() {
	brain := logic.NodeImpl{}
	rpcProxy := rpc_proxy.NewRPCImpl([]rpc_proxy.PeerRPCProxyConnectInfo{}, 1, "")
	rpcProxy.SetBrain(&brain)
}
