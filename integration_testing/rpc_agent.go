package integration_testing

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"net/rpc"
	"sync"
	"time"
)

type RpcAgentImpl struct {
	Log        observability.Logger
	peers      map[int]common.PeerRPCProxy
	peerParams []common.ClusterMember
}

type RpcServerConnectionInfo struct {
	ID  int
	URL string
}

func (r RpcAgentImpl) log() observability.Logger {
	return r.Log.With()
}

type NewRPCImplParams struct {
	Peers []common.ClusterMember
	Log   observability.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RpcAgentImpl, error) {
	r := RpcAgentImpl{Log: params.Log, peerParams: params.Peers}

	r.ConnectToRpcServers(params.Peers)

	return &r, nil
}

func (r *RpcAgentImpl) reconnectToRpcServer(peerID int, peerURL string) error {
	client, err := rpc.Dial("tcp", peerURL)
	if err != nil {
		return err
	} else {
		r.peers[peerID] = common.PeerRPCProxy{
			Conn: client,
			URL:  peerURL,
		}
	}

	return nil
}

func (r *RpcAgentImpl) ConnectToRpcServer(peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	if r.peers == nil {
		r.peers = map[int]common.PeerRPCProxy{}
	}

	for i := 0; i < retry; i++ {
		client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			time.Sleep(retryDelay)

			r.log().Error("ConnectToRpcServer: Client connection error: ", err)
		} else {
			r.log().Info(fmt.Sprintf("ConnectToRpcServer: connect to %s successfully", peerURL))
			r.peers[peerID] = common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			}

			timeout := 100 * time.Millisecond
			res, err := r.SendPing(peerID, &timeout)
			if err != nil {
				r.log().Error("ConnectToRpcServer: cannot ping", err, "url", peerURL)
			} else {
				r.log().Info("SendPing", "res", res)
				if res.ID != peerID {
					r.log().Error("connect to wrong server", nil)
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

func (r *RpcAgentImpl) disconnect(peerId int) error {
	peer, ok := r.peers[peerId]
	if !ok {
		return ErrPeerIdDoesNotExist
	}
	r.log().Info("Disconnect: rpc local agent", "serverUrl", peer.URL)

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
	ErrCannotReconnectToServer    = errors.New("cannot reconnect to server")
)

func (r RpcAgentImpl) callWithTimeout(peerID int, serviceMethod string, args any, reply any, timeout time.Duration) error {
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
			if err := r.reconnectToRpcServer(peerID, peer.URL); err != nil {
				return errors.Join(err, ErrCannotReconnectToServer)
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
		r.disconnect(peerID)
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			r.disconnect(peerID)
			return resp.Error
		}
	}

	return nil
}

func (r RpcAgentImpl) sendPingForInternalUse() {
}

func (r RpcAgentImpl) SendPing(peerId int, timeout *time.Duration) (responseMsg common.PingResponse, err error) {
	serviceMethod := "RPCProxyImpl.Ping"

	senderName := fmt.Sprintf("hello from Node %d", 0)

	if err := r.callWithTimeout(peerId, serviceMethod, senderName, &responseMsg, *timeout); err != nil {
		return responseMsg, err
	}

	return responseMsg, nil
}

func (r RpcAgentImpl) GetInfo(peerId int, timeout *time.Duration) (info common.GetStatusResponse, err error) {
	serviceMethod := "RPCProxyImpl.GetInfo"

	responseMsg := common.GetStatusResponse{}

	if err := r.callWithTimeout(peerId, serviceMethod, new(struct{}), &responseMsg, *timeout); err != nil {
		return info, err
	}
	return responseMsg, nil
}
