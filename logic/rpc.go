package logic

import (
	"net"
	"net/rpc"
)

func (n *NodeImpl) initRPC(url string) {
	err := rpc.Register(n)
	if err != nil {
		n.log().Fatal().Err(err).Msg("RPC handler: cannot register rpc")
	}

	listener, err := net.Listen("tcp", url)
	if err != nil {
		n.log().Fatal().Err(err).Msg("RPC handler: Listener error")
	}

	n.log().Info().Msg("RPC handler: finished register node")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				n.log().Err(err).Msg("RPC handler: listener error")
				continue
			}

			n.log().Info().Msg("RPC handler: received a request")

			go rpc.ServeConn(conn)
		}
	}()
}
