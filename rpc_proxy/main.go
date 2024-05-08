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
	GetInfo() common.GetStatusResponse
}

type RPCProxyImpl struct {
	peers      map[int]common.PeerRPCProxy
	hostID     int
	hostURL    string
	brain      RaftBrain
	rpcServer  *rpc.Server
	Log        *zerolog.Logger
	Stop       chan struct{}
	Accessible bool
	listener   net.Listener
	lock       sync.RWMutex
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r *RPCProxyImpl) log() *zerolog.Logger {
	l := r.Log.With().Int("RPC_ID", r.hostID).Str("RPC_URL", r.hostURL).Logger()
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
	Peers   []common.PeerInfo
	HostID  int
	HostURL string
	Log     *zerolog.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{
		hostID:     params.HostID,
		hostURL:    params.HostURL,
		Log:        params.Log,
		Stop:       make(chan struct{}),
		Accessible: true,
		peers:      make(map[int]common.PeerRPCProxy),
	}

	err := r.initRPCProxy(params.HostURL)
	if err != nil {
		return nil, err
	}
	// r.ConnectToPeers(params.Peers)

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

func (r *RPCProxyImpl) connectToPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			time.Sleep(retryDelay)

			r.log().Err(err).Msg("ConnectToPeers: Client connection error: ")
		} else {
			r.log().Info().Msgf("ConnectToPeers: connect to %s successfully", peerURL)
			r.setPeer(peerID, common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			})

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

	return nil
}

func (r *RPCProxyImpl) ConnectToPeers(params []common.PeerInfo) {
	var count sync.WaitGroup

	for _, peer := range params {
		if peer.ID == r.hostID {
			continue
		}

		count.Add(1)
		go func(peerURL string, peerID int) {
			r.connectToPeer(peerID, peerURL, 5, 3*time.Second)
			count.Done()
		}(peer.RpcUrl, peer.ID)
	}

	count.Wait()
}

func (r *RPCProxyImpl) disconnect(peerID int) error {
	peer, ok := r.peers[peerID]
	if !ok {
		return ErrPeerIdDoesNotExist
	}

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	return nil
}

func (r *RPCProxyImpl) disconnectAll() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, peer := range r.peers {
		if err := peer.Conn.Close(); err != nil {
			r.log().Err(err).Msg("DisconnectAll")
		}
	}

	return nil
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

	r.listener = listener

	r.log().Info().Msg("initRPCProxy: finished register node")
	go func() {
		for {
			<-r.Stop
			err := r.listener.Close()
			if err != nil {
				r.log().Err(err).Msg("RPC Proxy stop")
			}

			r.log().Info().Msg("RPC Proxy stop")
		}
	}()

	go func() {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				r.log().Err(err).Msg("RPCProxy: listener error")

				break
			} else {
				r.log().Info().Msg("RPCProxy: received a request")

				go r.rpcServer.ServeConn(conn)
			}
		}
		r.log().Err(err).Msg("RPCProxy: main loop stop")
	}()

	return nil
}

func (r *RPCProxyImpl) stopServer(peerIdx int) error {
	return r.listener.Close()
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
