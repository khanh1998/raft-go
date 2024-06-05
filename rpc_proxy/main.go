package rpc_proxy

import (
	"errors"
	"khanh/raft-go/common"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	ErrServerIdDoesNotMatch = errors.New("provided server ID does not match with connected server ID")
)

// the one which actually processes the request and makes all the decisions.
type RaftBrain interface {
	RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error)
	AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error)
	GetInfo() common.GetStatusResponse
	ToVotingMember() error
	GetNewMembersChannel() <-chan common.ClusterMemberChange
}

type RPCProxyImpl struct {
	peers       map[int]common.PeerRPCProxy
	hostID      int
	hostURL     string
	brain       RaftBrain
	rpcServer   *rpc.Server
	logger      *zerolog.Logger
	Stop        chan struct{}
	Accessible  bool
	listener    net.Listener
	lock        sync.RWMutex
	connections []net.Conn // TODO: unsafe, memory leak
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r *RPCProxyImpl) log() *zerolog.Logger {
	l := r.logger.With().Int("RPC_ID", r.hostID).Str("RPC_URL", r.hostURL).Logger()
	return &l
}

func (r *RPCProxyImpl) getPeer(peerId int) (common.PeerRPCProxy, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	peer, ok := r.peers[peerId]
	if !ok {
		return common.PeerRPCProxy{}, ErrPeerIdDoesNotExist
	}

	return peer, nil
}

func (r *RPCProxyImpl) setPeer(peerId int, peerInfo common.PeerRPCProxy) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.peers[peerId] = peerInfo
}

func (r *RPCProxyImpl) deletePeer(peerId int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	delete(r.peers, peerId)
}

type NewRPCImplParams struct {
	HostID  int
	HostURL string
	Log     *zerolog.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{
		hostID:     params.HostID,
		hostURL:    params.HostURL,
		logger:     params.Log,
		Stop:       make(chan struct{}),
		Accessible: true,
		peers:      make(map[int]common.PeerRPCProxy),
	}

	return &r, nil
}

func (r *RPCProxyImpl) SetBrain(brain RaftBrain) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.brain = brain
}

func (r *RPCProxyImpl) ConnectToNewPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	return r.connectToPeer(peerID, peerURL, retry, retryDelay)
}

func (r *RPCProxyImpl) DisconnectToAllPeers() {
	for id := range r.peers {
		r.disconnectToPeer(id)
	}

	r.peers = map[int]common.PeerRPCProxy{}
}

func (r *RPCProxyImpl) disconnectToPeer(peerID int) error {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return err
	}

	r.log().Info().Int("id", r.hostID).Msg("disconnectToPeer")

	r.setPeer(peerID, common.PeerRPCProxy{Conn: nil, URL: peer.URL})

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	return nil
}

func (r *RPCProxyImpl) connectToPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	timeout := 150 * time.Millisecond

	// an connection exist
	peer, err := r.getPeer(peerID)
	if err == nil && peer.Conn != nil {
		res, err := r.SendPing(peerID, &timeout)
		if err == nil {
			if res.ID == peerID {
				return nil
			}
		}
	}

	// non connection exist
	r.setPeer(peerID, common.PeerRPCProxy{
		Conn: nil,
		URL:  peerURL,
	})

	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			r.log().Err(err).Msgf("ConnectToPeer: can't connect to %s ", peerURL)

			time.Sleep(retryDelay)
		} else {
			r.log().Info().Msgf("ConnectToPeer: connect to %s successfully", peerURL)

			r.setPeer(peerID, common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			})

			res, err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Err(err).Msgf("ConnectToPeer: cannot ping %s", peerURL)
			} else {
				if res.ID != peerID {
					return ErrServerIdDoesNotMatch
				}
			}

			break
		}

	}

	return nil
}

func (r *RPCProxyImpl) initServer(url string) error {
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

	r.listener = listener

	go func() {
		<-r.Stop
		err := r.listener.Close()
		if err != nil {
			r.log().Err(err).Msg("RPC Proxy stopping triggered")
		}

		r.log().Info().Msg("RPC Proxy stopping triggered")

		r.lock.Lock()
		defer r.lock.Unlock()

		for _, conn := range r.connections {
			if err := conn.Close(); err != nil {
				r.log().Err(err).Msg("RPC Proxy stopping: close connection")
			}
		}

		r.connections = []net.Conn{} // delete all closed connnections
	}()

	go func() {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				r.log().Err(err).Msg("RPCProxy: listener error")

				break
			} else {
				r.lock.Lock()
				r.connections = append(r.connections, conn)
				r.lock.Unlock()
				r.log().Info().
					Int("total", len(r.connections)).
					Str("remoteAddr", conn.RemoteAddr().String()).
					Str("localAddr", conn.LocalAddr().String()).
					Msg("RPCProxy: new connection created")

				go r.rpcServer.ServeConn(conn)
			}

		}

		r.log().Info().Msg("RPCProxy: main loop stop")
	}()
	r.log().Info().Msg("initRPCProxy: finished register node")

	return nil
}

func (r *RPCProxyImpl) Start() {
	if err := r.initServer(r.hostURL); err != nil {
		r.log().Panic().Err(err).Msg("Start RPC Proxy")
	}

	// waiting for member changes
	go func() {
		for member := range r.brain.GetNewMembersChannel() {
			if r.hostID != member.ID {
				if member.Add {
					log.Info().Interface("member", member).Msg("connect new member")
					r.connectToPeer(member.ID, member.RpcUrl, 5, 150*time.Millisecond)
				} else {
					log.Info().Interface("member", member).Msg("disconnect new member")
					r.disconnectToPeer(member.ID)
				}
			}
		}
	}()

}

func (r *RPCProxyImpl) reconnect(peerIdx int) error {
	peer, err := r.getPeer(peerIdx)
	if err != nil {
		return err
	}
	targetUrl := peer.URL

	r.log().Info().Int("id", r.hostID).Str("target", targetUrl).Msg("reconnect")

	client, err := rpc.Dial("tcp", targetUrl)
	if err != nil {
		return err
	}

	r.setPeer(peerIdx, common.PeerRPCProxy{
		Conn: client,
		URL:  targetUrl,
	})

	timeout := 150 * time.Millisecond
	res, err := r.SendPing(peerIdx, &timeout)
	if err != nil {
		r.log().Err(err).Msgf("ConnectToPeer: cannot ping %s", targetUrl)
	} else {
		if res.ID != peerIdx {
			return ErrServerIdDoesNotMatch
		}
	}

	return nil
}

var (
	ErrPeerIdDoesNotExist      = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull = errors.New("rpc peer connection is nil")
	ErrRpcTimeout              = errors.New("rpc call take too long")
)

func (r *RPCProxyImpl) callWithoutTimeout(peerID int, serviceMethod string, args any, reply any) (err error) {
	var peer common.PeerRPCProxy

	for i := 0; i < 2; i++ {
		peer, err = r.getPeer(peerID)
		if err != nil {
			return err
		}

		if peer.Conn == nil {
			err := r.connectToPeer(peerID, peer.URL, 1, 150*time.Millisecond)
			if err != nil {
				return err
			}
		} else {
			break
		}
	}

	if peer.Conn != nil {
		return peer.Conn.Call(serviceMethod, args, reply)
	}
	return ErrRpcPeerConnectionIsNull
}

func (r *RPCProxyImpl) callWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) (err error) {
	var peer common.PeerRPCProxy

	for i := 0; i < 2; i++ {
		peer, err = r.getPeer(peerID)
		if err != nil {
			return err
		}

		if peer.Conn == nil {
			r.reconnect(peerID)
		} else {
			break
		}
	}

	if peer.Conn != nil {
		call := peer.Conn.Go(serviceMethod, args, reply, nil)
		select {
		case <-time.After(timeout):
			r.disconnectToPeer(peerID)
			return ErrRpcTimeout
		case resp := <-call.Done:
			if resp != nil && resp.Error != nil {
				r.disconnectToPeer(peerID)
				return resp.Error
			}
		}

		return nil
	}
	return ErrRpcPeerConnectionIsNull
}
