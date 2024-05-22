package intergration_test

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type RPCProxyImpl struct {
	Log        *zerolog.Logger
	peers      map[int]common.PeerRPCProxy
	peerParams []common.ClusterMember
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r RPCProxyImpl) log() *zerolog.Logger {
	l := r.Log.With().Int("RPC_ID", 0).Str("RPC_URL", "test").Logger()
	return &l
}

type NewRPCImplParams struct {
	Peers []common.ClusterMember
	Log   *zerolog.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{Log: params.Log, peerParams: params.Peers}

	r.ConnectToPeers(params.Peers)

	return &r, nil
}

func (r *RPCProxyImpl) ConnectToPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	if r.peers == nil {
		r.peers = map[int]common.PeerRPCProxy{}
	}

	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			time.Sleep(retryDelay)

			r.log().Err(err).Msg("ConnectToPeer: Client connection error: ")
		} else {
			r.log().Info().Msgf("ConnectToPeer: connect to %s successfully", peerURL)
			r.peers[peerID] = common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			}

			var message string

			timeout := 5 * time.Second

			err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Err(err).Str("url", peerURL).Msg("ConnectToPeer: cannot ping")
			} else {
				r.log().Info().Msg(message)
			}

			break
		}
	}

	return nil
}

func (r *RPCProxyImpl) ConnectToPeers(params []common.ClusterMember) {
	r.peers = make(map[int]common.PeerRPCProxy)

	var count sync.WaitGroup

	for _, peer := range params {
		count.Add(1)
		go func(peerURL string, peerID int) {
			r.ConnectToPeer(peerID, peerURL, 5, 3*time.Second)
			count.Done()
		}(peer.RpcUrl, peer.ID)
	}

	count.Wait()
}

func (r *RPCProxyImpl) Disconnect(peerId int) error {
	peer, ok := r.peers[peerId]
	if !ok {
		return ErrPeerIdDoesNotExist
	}

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	r.peers[peerId] = common.PeerRPCProxy{URL: peer.URL}

	return nil
}

func (r RPCProxyImpl) Reconnect(peerId int) error {
	var peer *common.ClusterMember
	for _, p := range r.peerParams {
		if peerId == p.ID {
			peer = &p
		}
	}

	if peer == nil {
		return ErrPeerIdDoesNotExist
	}

	targetUrl := peer.RpcUrl

	client, err := rpc.Dial("tcp", targetUrl)
	if err != nil {
		return err
	}

	var message string
	err = client.Call("RPCProxyImpl.Ping", fmt.Sprintf("Node %v", 0), &message)
	if err != nil {
		r.log().Err(err).Str("url", targetUrl).Msg("Reconnect: cannot ping")

		return err
	} else {
		r.log().Info().Msg(message)
	}

	r.peers[peerId] = common.PeerRPCProxy{
		Conn: client,
		URL:  targetUrl,
	}

	return nil
}

var (
	ErrPeerIdDoesNotExist      = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull = errors.New("rpc peer connection is nil")
)

func (r RPCProxyImpl) CallWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
	var (
		peer common.PeerRPCProxy
		ok   bool
	)

	peer, ok = r.peers[peerID]
	if !ok || peer.Conn == nil {
		if err := r.Reconnect(peerID); err != nil {
			return errors.Join(err, ErrPeerIdDoesNotExist)
		}
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
			// r.Reconnect(peerID)
			return resp.Error
		}
	}

	return nil
}

func (r RPCProxyImpl) SendPing(peerId int, timeout *time.Duration) (err error) {
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", 0)
	responseMsg := ""

	if err := r.CallWithTimeout(peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
		return err
	}
	return nil
}

func (r RPCProxyImpl) GetInfo(peerId int, timeout *time.Duration) (info common.GetStatusResponse, err error) {
	serviceMethod := "RPCProxyImpl.GetInfo"

	responseMsg := common.GetStatusResponse{}

	if err := r.CallWithTimeout(peerId, serviceMethod, new(struct{}), &responseMsg, *timeout); err != nil {
		return info, err
	}
	return responseMsg, nil
}
