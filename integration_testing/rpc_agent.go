package integration_testing

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type RpcAgentImpl struct {
	Log        *zerolog.Logger
	peers      map[int]common.PeerRPCProxy
	peerParams []common.ClusterMember
}

type RpcServerConnectionInfo struct {
	ID  int
	URL string
}

func (r RpcAgentImpl) log() *zerolog.Logger {
	l := r.Log.With().Int("RPC_ID", 0).Str("RPC_URL", "test").Logger()
	return &l
}

type NewRPCImplParams struct {
	Peers []common.ClusterMember
	Log   *zerolog.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RpcAgentImpl, error) {
	r := RpcAgentImpl{Log: params.Log, peerParams: params.Peers}

	r.ConnectToPeers(params.Peers)

	return &r, nil
}

func (r *RpcAgentImpl) ConnectToPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
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

			timeout := 500 * time.Millisecond
			res, err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Err(err).Str("url", peerURL).Msg("ConnectToPeer: cannot ping")
			} else {
				r.log().Info().Interface("res", res).Msg("SendPing")
				if res.ID != peerID {
					r.log().Err(nil).Msg("connect to wrong server")
					return errors.New("connect to wrong server, input value is incorrect")
				}
			}

			break
		}
	}

	return nil
}

func (r *RpcAgentImpl) ConnectToPeers(params []common.ClusterMember) {
	r.peers = make(map[int]common.PeerRPCProxy)

	var count sync.WaitGroup

	for _, peer := range params {
		count.Add(1)
		go func(peerURL string, peerID int) {
			r.ConnectToPeer(peerID, peerURL, 5, 150*time.Millisecond)
			count.Done()
		}(peer.RpcUrl, peer.ID)
	}

	count.Wait()
}

func (r *RpcAgentImpl) Disconnect(peerId int) error {
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

func (r RpcAgentImpl) Reconnect(peerId int) error {
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

	timeout := 500 * time.Millisecond
	res, err := r.SendPing(peerId, &timeout)
	if err != nil {
		r.log().Err(err).Str("url", peer.RpcUrl).Msg("ConnectToPeer: cannot ping")
	} else {
		r.log().Info().Interface("res", res).Msg("SendPing")
		if res.ID != peerId {
			r.log().Err(nil).Msg("connect to wrong server")
			return errors.New("connect to wrong server, input value is incorrect")
		}
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

func (r RpcAgentImpl) CallWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
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

func (r RpcAgentImpl) SendPing(peerId int, timeout *time.Duration) (responseMsg common.PingResponse, err error) {
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", 0)

	if err := r.CallWithTimeout(peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
		return responseMsg, err
	}

	return responseMsg, nil
}

func (r RpcAgentImpl) GetInfo(peerId int, timeout *time.Duration) (info common.GetStatusResponse, err error) {
	serviceMethod := "RPCProxyImpl.GetInfo"

	responseMsg := common.GetStatusResponse{}

	if err := r.CallWithTimeout(peerId, serviceMethod, new(struct{}), &responseMsg, *timeout); err != nil {
		return info, err
	}
	return responseMsg, nil
}
