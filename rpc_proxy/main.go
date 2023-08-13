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
	peers     map[int]PeerRPCProxy // TODO: data race
	hostID    int
	hostURL   string
	brain     RaftBrain
	rpcServer *rpc.Server
	Log       *zerolog.Logger
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r RPCProxyImpl) log() *zerolog.Logger {
	l := r.Log.With().Int("RPC_ID", r.hostID).Str("RPC_URL", r.hostURL).Logger()
	return &l
}

type NewRPCImplParams struct {
	Peers   []common.PeerInfo
	HostID  int
	HostURL string
	Log     *zerolog.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{hostID: params.HostID, hostURL: params.HostURL, Log: params.Log}

	err := r.initRPCProxy(params.HostURL)
	if err != nil {
		return nil, err
	}
	// r.ConnectToPeers(params.Peers)

	return &r, nil
}

func (r *RPCProxyImpl) SetBrain(brain RaftBrain) {
	r.brain = brain
}

func (r *RPCProxyImpl) ConnectToPeers(params []common.PeerInfo) {
	r.peers = make(map[int]PeerRPCProxy)

	var count sync.WaitGroup

	for _, peer := range params {
		if peer.ID == r.hostID {
			continue
		}

		count.Add(1)
		go func(peerURL string, peerID int) {
			for i := 0; i < 5; i++ {
				r.log().Info().Msgf("ConnectToPeers: dialing %s", peerURL)
				client, err := rpc.Dial("tcp", peerURL)
				if err != nil {
					time.Sleep(3 * time.Second)
					r.log().Err(err).Msg("ConnectToPeers: Client connection error: ")
					continue
				} else {
					r.log().Info().Msgf("ConnectToPeers: connect to %s successfully", peerURL)
					r.peers[peerID] = PeerRPCProxy{
						conn: client,
						url:  peerURL,
					}

					var message string

					timeout := 5 * time.Second

					err := r.SendPing(peerID, &timeout)
					if err != nil {
						r.log().Err(err).Str("url", peerURL).Msg("ConnectToPeers: cannot ping")
					} else {
						r.log().Info().Msg(message)
					}

					break
				}

			}
			count.Done()
		}(peer.URL, peer.ID)
	}

	count.Wait()
}

func (r *RPCProxyImpl) initRPCProxy(url string) error {
	r.rpcServer = rpc.NewServer()
	if err := r.rpcServer.RegisterName("RPCProxyImpl", r); err != nil {
		r.log().Err(err).Msg("initRPCProxy")

		return err
	}

	listener, err := net.Listen("tcp", url)
	if err != nil {
		r.log().Fatal().Err(err).Msg("initRPCProxy: Listener error")

		return err
	}

	r.log().Info().Msg("initRPCProxy: finished register node")

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				r.log().Err(err).Msg("initRPCProxy: listener error")
				continue
			}

			r.log().Info().Msg("initRPCProxy: received a request")

			go r.rpcServer.ServeConn(conn)
		}
	}()

	return nil
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
		r.log().Err(err).Str("url", targetUrl).Msg("Reconnect: cannot ping")

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
