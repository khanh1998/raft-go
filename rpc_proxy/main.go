package rpc_proxy

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// the one which actually processes the request and makes all the decisions.
type RaftBrain interface {
	RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error)
	AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error)
}

type PeerRPCProxy struct {
	conn *rpc.Client
	url  string
}

type RPCProxyImpl struct {
	peers   map[int]PeerRPCProxy
	hostID  int
	hostURL string
	brain   RaftBrain
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r RPCProxyImpl) log() *zerolog.Logger {
	l := log.With().Int("RPC ID", r.hostID).Str("RPC URL", r.hostURL).Logger()
	return &l
}

func NewRPCImpl(params []PeerRPCProxyConnectInfo, hostID int, hostURL string) RPCProxyImpl {
	r := RPCProxyImpl{hostID: hostID, hostURL: hostURL}

	r.initRPCProxy(hostURL)
	r.ConnectToPeers(params)

	return r
}

func (r *RPCProxyImpl) SetBrain(brain RaftBrain) {
	r.brain = brain
}

func (r *RPCProxyImpl) ConnectToPeers(params []PeerRPCProxyConnectInfo) {
	r.peers = make(map[int]PeerRPCProxy)

	var count sync.WaitGroup

	for _, param := range params {
		count.Add(1)
		go func(url string, index int) {
			for i := 0; i < 5; i++ {
				r.log().Info().Msgf("dialing %s", url)
				client, err := rpc.Dial("tcp", url)
				if err != nil {
					time.Sleep(3 * time.Second)
					r.log().Err(err).Msg("Client connection error: ")
					continue
				} else {
					r.log().Info().Msgf("connect to %s successfully", url)
					r.peers[index] = PeerRPCProxy{
						conn: client,
						url:  url,
					}

					var message string
					err = client.Call("NodeImpl.Ping", fmt.Sprintf("Node %v", r.hostID), &message)
					if err != nil {
						log.Err(err).Str("url", url).Msg("cannot ping")
					} else {
						log.Info().Msg(message)
					}

					break
				}

			}
			count.Done()
		}(param.URL, param.ID)
	}

	count.Wait()
}

func (r *RPCProxyImpl) initRPCProxy(url string) {
	err := rpc.Register(r)
	if err != nil {
		r.log().Fatal().Err(err).Msg("RPC handler: cannot register rpc")
	}

	listener, err := net.Listen("tcp", url)
	if err != nil {
		r.log().Fatal().Err(err).Msg("RPC handler: Listener error")
	}

	r.log().Info().Msg("RPC handler: finished register node")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				r.log().Err(err).Msg("RPC handler: listener error")
				continue
			}

			r.log().Info().Msg("RPC handler: received a request")

			go rpc.ServeConn(conn)
		}
	}()
}

func (r RPCProxyImpl) Reconnect(peerIdx int) error {
	targetUrl := r.peers[peerIdx].url

	client, err := rpc.Dial("tcp", targetUrl)
	if err != nil {
		return err
	}

	var message string
	err = client.Call("RPCProxyImpl.Ping", fmt.Sprintf("Node %v", r.hostID), &message)
	if err != nil {
		r.log().Err(err).Str("url", targetUrl).Msg("Reconnect peer: cannot ping")

		return err
	} else {
		r.log().Info().Msg(message)
	}

	r.peers[peerIdx] = PeerRPCProxy{
		conn: client,
		url:  targetUrl,
	}

	return nil
}

var (
	ErrPeerIdDoesNotExist      = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull = errors.New("rpc peer connection is nil")
)

func (r RPCProxyImpl) CallWithoutTimeout(peerID int, serviceMethod string, args any, reply any) error {
	var (
		peer PeerRPCProxy
		ok   bool
	)

	peer, ok = r.peers[peerID]
	if !ok {
		return ErrPeerIdDoesNotExist
	}

	if peer.conn == nil {
		return ErrRpcPeerConnectionIsNull
	}

	return peer.conn.Call(serviceMethod, args, reply)
}

func (r RPCProxyImpl) CallWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
	var (
		peer PeerRPCProxy
		ok   bool
	)

	peer, ok = r.peers[peerID]
	if !ok {
		return ErrPeerIdDoesNotExist
	}

	if peer.conn == nil {
		return ErrRpcPeerConnectionIsNull
	}

	call := peer.conn.Go(serviceMethod, args, reply, nil)
	select {
	case <-time.After(timeout):
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			r.Reconnect(peerID)
			return resp.Error
		}
	}

	return nil
}
