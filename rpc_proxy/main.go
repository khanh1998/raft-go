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

func (r *RPCProxyImpl) AddPeerToStaging(peerID int, peerURL string, retry int, retryDelay time.Duration) error {

	return nil
}

func (r *RPCProxyImpl) CommitStaging(peerID int, peerURL string, retry int, retryDelay time.Duration) error {

	return nil
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

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	delete(r.peers, peerID) // TODO: data race

	return nil
}

func (r *RPCProxyImpl) connectToPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			r.log().Err(err).Msg("ConnectToPeer: Client connection error: ")

			time.Sleep(retryDelay)
		} else {
			r.log().Info().Msgf("ConnectToPeer: connect to %s successfully", peerURL)
			r.setPeer(peerID, common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			})

			timeout := 5 * time.Second

			res, err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Err(err).Str("url", peerURL).Msg("ConnectToPeer: cannot ping")
			} else {
				if res.ID != peerID {
					return ErrServerIdDoesNotMatch
				}

				r.log().Info().Interface("response", res).Msg("SendPing")
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
				r.log().Info().Msg("RPCProxy: new connection created")

				r.lock.Lock()
				r.connections = append(r.connections, conn)
				r.lock.Unlock()

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
					r.connectToPeer(member.ID, member.RpcUrl, 5, 5*time.Second)
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

	r.setPeer(peerIdx, common.PeerRPCProxy{
		Conn: client,
		URL:  targetUrl,
	})

	return nil
}

var (
	ErrPeerIdDoesNotExist      = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull = errors.New("rpc peer connection is nil")
)

func (r *RPCProxyImpl) callWithoutTimeout(peerID int, serviceMethod string, args any, reply any) error {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return err
	}

	if peer.Conn == nil {
		return ErrRpcPeerConnectionIsNull
	}

	return peer.Conn.Call(serviceMethod, args, reply)
}

func (r *RPCProxyImpl) callWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return err
	}

	if peer.Conn == nil {
		return ErrRpcPeerConnectionIsNull
	}

	call := peer.Conn.Go(serviceMethod, args, reply, nil)
	select {
	case <-time.After(timeout):
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			r.reconnect(peerID)
			return resp.Error
		}
	}

	return nil
}
