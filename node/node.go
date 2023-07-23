package node

import (
	"khanh/raft-go/http_proxy"
	"khanh/raft-go/logic"
	"khanh/raft-go/rpc_proxy"

	"github.com/rs/zerolog/log"
)

type Node struct {
}

type NewNodeParams struct {
	Brain     logic.NewNodeParams
	RPCProxy  rpc_proxy.NewRPCImplParams
	HTTPProxy http_proxy.NewHttpProxyParams
}

func NewNode(params NewNodeParams) {
	brain, err := logic.NewNode(params.Brain)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	rpcProxy := rpc_proxy.NewRPCImpl(params.RPCProxy)
	httpProxy := http_proxy.NewHttpProxy(params.HTTPProxy)
	rpcProxy.SetBrain(brain)
	brain.SetRpcProxy(rpcProxy)
	httpProxy.SetBrain(brain)
}
