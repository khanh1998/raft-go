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

	r.ConnectToRpcServers(params.Peers)

	return &r, nil
}

func (r *RpcAgentImpl) ConnectToRpcServer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	if r.peers == nil {
		r.peers = map[int]common.PeerRPCProxy{}
	}

	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			time.Sleep(retryDelay)

			r.log().Err(err).Msg("ConnectToRpcServer: Client connection error: ")
		} else {
			r.log().Info().Msgf("ConnectToRpcServer: connect to %s successfully", peerURL)
			r.peers[peerID] = common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			}

			timeout := 100 * time.Millisecond
			res, err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Err(err).Str("url", peerURL).Msg("ConnectToRpcServer: cannot ping")
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

func (r *RpcAgentImpl) ConnectToRpcServers(params []common.ClusterMember) {
	r.peers = make(map[int]common.PeerRPCProxy)

	var count sync.WaitGroup

	for _, peer := range params {
		count.Add(1)
		go func(peerURL string, peerID int) {
			r.ConnectToRpcServer(peerID, peerURL, 1, 150*time.Millisecond)
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
	r.log().Info().Str("serverUrl", peer.URL).Msg("Disconnect: rpc local agent")

	r.peers[peerId] = common.PeerRPCProxy{URL: peer.URL, Conn: nil}

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	return nil
}

var (
	ErrPeerIdDoesNotExist         = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull    = errors.New("rpc peer connection is nil")
	ErrNoConnectionInfoCanBeFound = errors.New("no connection info can be found")
)

func (r RpcAgentImpl) CallWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
	var (
		peer common.PeerRPCProxy
		ok   bool
	)

	for i := 0; i < 2; i++ {
		peer, ok = r.peers[peerID]
		if !ok {
			return ErrNoConnectionInfoCanBeFound
		}

		if peer.Conn == nil {
			if err := r.ConnectToRpcServer(peerID, peer.URL, 1, 100*time.Millisecond); err != nil {
				return errors.Join(err, ErrPeerIdDoesNotExist)
			}
		} else {
			break
		}
	}

	if peer.Conn == nil {
		return ErrRpcPeerConnectionIsNull
	}

	call := peer.Conn.Go(serviceMethod, args, reply, nil)
	select {
	case <-time.After(timeout):
		r.Disconnect(peerID)
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			r.Disconnect(peerID)
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
