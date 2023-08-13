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
	Brain     logic.NewRaftBrainParams
	RPCProxy  rpc_proxy.NewRPCImplParams
	HTTPProxy http_proxy.NewHttpProxyParams
}

func NewNode(params NewNodeParams) {
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
}
