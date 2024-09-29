package rpc_proxy

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"net"
	"net/rpc"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
)

var (
	ErrServerIdDoesNotMatch = errors.New("provided server ID does not match with connected server ID")

	tracer = otel.Tracer("rpc-proxy")
)

// the one which actually processes the request and makes all the decisions.
type RaftBrain interface {
	RequestVote(ctx context.Context, input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error)
	AppendEntries(ctx context.Context, input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error)
	GetInfo() common.GetStatusResponse
	ToVotingMember(ctx context.Context) error
	GetNewMembersChannel() <-chan common.ClusterMemberChange
}

type RPCProxyImpl struct {
	peers       map[int]common.PeerRPCProxy
	hostID      int
	hostURL     string
	brain       RaftBrain
	rpcServer   *rpc.Server
	logger      observability.Logger
	stop        chan struct{}
	accessible  bool
	listener    net.Listener
	lock        sync.RWMutex
	connections []net.Conn // TODO: unsafe, memory leak
}

type PeerRPCProxyConnectInfo struct {
	ID  int
	URL string
}

func (r *RPCProxyImpl) log() observability.Logger {
	l := r.logger.With(
		"RPC_ID", r.hostID,
		"RPC_URL", r.hostURL,
	)

	return l
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
	Logger  observability.Logger
}

func NewRPCImpl(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{
		hostID:     params.HostID,
		hostURL:    params.HostURL,
		logger:     params.Logger,
		stop:       make(chan struct{}),
		accessible: true,
		peers:      make(map[int]common.PeerRPCProxy),
	}

	return &r, nil
}

func (r *RPCProxyImpl) SetBrain(brain RaftBrain) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.brain = brain
}

func (r *RPCProxyImpl) ConnectToNewPeer(ctx context.Context, peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	return r.connectToPeer(ctx, peerID, peerURL, retry, retryDelay)
}

func (r *RPCProxyImpl) DisconnectToAllPeers(ctx context.Context) {
	for id := range r.peers {
		r.disconnectToPeer(ctx, id)
	}

	r.peers = map[int]common.PeerRPCProxy{}
}

func (r *RPCProxyImpl) disconnectToPeer(ctx context.Context, peerID int) error {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return err
	}

	r.log().DebugContext(ctx, "disconnectToPeer")

	r.setPeer(peerID, common.PeerRPCProxy{Conn: nil, URL: peer.URL})

	if err := peer.Conn.Close(); err != nil {
		return err
	}

	return nil
}

func (r *RPCProxyImpl) connectToPeer(ctx context.Context, peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	timeout := 150 * time.Millisecond

	// an connection exist
	peer, err := r.getPeer(peerID)
	if err == nil && peer.Conn != nil {
		res, err := r.SendPing(ctx, peerID, &timeout)
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
			r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: can't connect to %s ", peerURL), err)

			time.Sleep(retryDelay)
		} else {
			r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: connect to %s successfully", peerURL), err)

			r.setPeer(peerID, common.PeerRPCProxy{
				Conn: client,
				URL:  peerURL,
			})

			res, err := r.SendPing(ctx, peerID, &timeout)
			if err != nil {
				r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: cannot ping %s", peerURL), err)
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

func (r *RPCProxyImpl) initServer(ctx context.Context, url string) error {
	r.rpcServer = rpc.NewServer()
	if err := r.rpcServer.RegisterName("RPCProxyImpl", r); err != nil {
		r.log().ErrorContext(ctx, "initRPCProxy", err)

		return err
	}

	listener, err := net.Listen("tcp", url)
	if err != nil {
		r.log().FatalContext(ctx, "initRPCProxy: Listener error")

		return err
	}

	r.listener = listener

	go func() {
		<-r.stop
		err := r.listener.Close()
		if err != nil {
			r.log().ErrorContext(ctx, "initRPCProxy: Listener error", err)
		}

		r.log().InfoContext(ctx, "RPC Proxy stopping triggered")

		r.lock.Lock()
		defer r.lock.Unlock()

		for _, conn := range r.connections {
			if err := conn.Close(); err != nil {
				r.log().ErrorContext(ctx, "RPC Proxy stopping: close connection", err)
			}
		}

		r.connections = []net.Conn{} // delete all closed connnections
	}()

	go func() {
		for {
			conn, err := r.listener.Accept()
			if err != nil {
				r.log().ErrorContext(ctx, "RPCProxy: listener error", err)

				break
			} else {
				r.lock.Lock()
				r.connections = append(r.connections, conn)
				r.lock.Unlock()
				r.log().DebugContext(
					ctx,
					"RPCProxy: new connection created",
					"total", len(r.connections),
					"remoteAddr", conn.RemoteAddr().String(),
					"localAddr", conn.LocalAddr().String(),
				)

				go r.rpcServer.ServeConn(conn)
			}

		}

		r.log().InfoContext(ctx, "RPCProxy: main loop stop")
	}()

	r.log().InfoContext(ctx, "initRPCProxy: finished register node")

	return nil
}

func (r *RPCProxyImpl) Start(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Start RPC Server")
	defer span.End()

	if err := r.initServer(ctx, r.hostURL); err != nil {
		r.log().FatalContext(ctx, "Start RPC Proxy", "error", err.Error())
	}

	// waiting for member changes
	go func() {
		for member := range r.brain.GetNewMembersChannel() {
			if r.hostID != member.ID {
				if member.Add {
					r.logger.Info("connect new member", "member", member)
					r.connectToPeer(ctx, member.ID, member.RpcUrl, 5, 150*time.Millisecond)
				} else {
					r.logger.Info("disconnect new member", "member", member)
					r.disconnectToPeer(ctx, member.ID)
				}
			}
		}
	}()

}

func (r *RPCProxyImpl) reconnect(ctx context.Context, peerIdx int) error {
	peer, err := r.getPeer(peerIdx)
	if err != nil {
		return err
	}
	targetUrl := peer.URL

	r.log().InfoContext(ctx, "reconnect", "targetURL", targetUrl)

	client, err := rpc.Dial("tcp", targetUrl)
	if err != nil {
		return err
	}

	r.setPeer(peerIdx, common.PeerRPCProxy{
		Conn: client,
		URL:  targetUrl,
	})

	timeout := 150 * time.Millisecond
	res, err := r.SendPing(ctx, peerIdx, &timeout)
	if err != nil {
		r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: cannot ping %s", targetUrl), err)
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

func (r *RPCProxyImpl) callWithoutTimeout(ctx context.Context, peerID int, serviceMethod string, args any, reply any) (err error) {
	var peer common.PeerRPCProxy

	for i := 0; i < 2; i++ {
		peer, err = r.getPeer(peerID)
		if err != nil {
			return err
		}

		if peer.Conn == nil {
			err := r.connectToPeer(ctx, peerID, peer.URL, 1, 150*time.Millisecond)
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

func (r *RPCProxyImpl) callWithTimeout(ctx context.Context, peerID int, serviceMethod string, args any, reply any, timeout time.Duration) (err error) {
	var peer common.PeerRPCProxy

	for i := 0; i < 2; i++ {
		peer, err = r.getPeer(peerID)
		if err != nil {
			return err
		}

		if peer.Conn == nil {
			r.reconnect(ctx, peerID)
		} else {
			break
		}
	}

	if peer.Conn != nil {
		call := peer.Conn.Go(serviceMethod, args, reply, nil)
		select {
		case <-time.After(timeout):
			r.disconnectToPeer(ctx, peerID)
			return ErrRpcTimeout
		case resp := <-call.Done:
			if resp != nil && resp.Error != nil {
				r.disconnectToPeer(ctx, peerID)
				return resp.Error
			}
		}

		return nil
	}
	return ErrRpcPeerConnectionIsNull
}

func (r *RPCProxyImpl) SetAccessible() {
	r.accessible = true
}

func (r *RPCProxyImpl) SetInaccessible() {
	r.accessible = false
}

func (r *RPCProxyImpl) Stop() {
	select {
	case r.stop <- struct{}{}:
	default:
	}
}
