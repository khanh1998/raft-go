package logic

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
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

func (n *NodeImpl) Reconnect(peerIdx int) error {
	client, err := rpc.Dial("tcp", n.PeerURLs[peerIdx])
	if err != nil {
		return err
	}

	var message string
	err = client.Call("NodeImpl.Ping", fmt.Sprintf("Node %v", n.ID), &message)
	if err != nil {
		log.Err(err).Str("url", n.PeerURLs[peerIdx]).Msg("Reconnect peer: cannot ping")

		return err
	} else {
		log.Info().Msg(message)
	}

	n.Peers[peerIdx] = client

	return nil
}

func (n *NodeImpl) ConnectToPeers() {
	n.Peers = make([]*rpc.Client, len(n.PeerURLs))
	var count sync.WaitGroup
	for idx, url := range n.PeerURLs {
		count.Add(1)
		go func(url string, index int) {
			for i := 0; i < 5; i++ {
				n.log().Info().Msgf("dialing %s", url)
				client, err := rpc.Dial("tcp", url)
				if err != nil {
					time.Sleep(3 * time.Second)
					n.log().Err(err).Msg("Client connection error: ")
					continue
				} else {
					n.log().Info().Msgf("connect to %s successfully", url)
					n.Peers[index] = client

					var message string
					err = client.Call("NodeImpl.Ping", fmt.Sprintf("Node %v", n.ID), &message)
					if err != nil {
						log.Err(err).Str("url", url).Msg("cannot ping")
					} else {
						log.Info().Msg(message)
					}

					break
				}

			}
			count.Done()
		}(url, idx)
	}

	count.Wait()

	if len(n.Peers) == len(n.PeerURLs) {
		n.log().Info().Msg("Successfully connected to all peers")
	} else {
		n.log().Fatal().Msg("cannot connect to all peers")
	}

	n.resetElectionTimeout()
	n.resetHeartBeatTimeout()
}
