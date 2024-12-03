package rpc_proxy

import (
	"context"
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/common"
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
	InstallSnapshot(ctx context.Context, input *common.InstallSnapshotInput, output *common.InstallSnapshotOutput)
	GetInfo() gc.GetStatusResponse
	ToVotingMember(ctx context.Context) error
	GetNewMembersChannel() <-chan common.ClusterMemberChange
}

type RPCProxyImpl struct {
	peers                map[int]gc.PeerRPCProxy
	hostID               int
	hostURL              string
	brain                RaftBrain
	rpcServer            *rpc.Server
	logger               observability.Logger
	stop                 context.CancelFunc // stop background goroutines
	accessible           bool
	listener             net.Listener
	lock                 sync.RWMutex
	rpcDialTimeout       time.Duration
	rpcReconnectDuration time.Duration
	rpcRequestTimeout    time.Duration
}

func (r *RPCProxyImpl) log() observability.Logger {
	l := r.logger.With(
		"source", "RPCProxy",
		"RPC_ID", r.hostID,
		"peers", r.peers,
	)

	return l
}

func (r *RPCProxyImpl) getPeer(peerId int) (gc.PeerRPCProxy, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	peer, ok := r.peers[peerId]
	if !ok {
		return gc.PeerRPCProxy{}, ErrPeerIdDoesNotExist
	}

	return peer, nil
}

// only accept new peer info if it haven't get registered,
// or the peer has new URL
func (r *RPCProxyImpl) setPeerIfNew(peerId int, peerInfo gc.PeerRPCProxy) {
	r.lock.Lock()
	defer r.lock.Unlock()

	old, ok := r.peers[peerId]
	if !ok {
		r.peers[peerId] = peerInfo
	} else if old.URL != peerInfo.URL {
		r.peers[peerId] = peerInfo
	}
}

func (r *RPCProxyImpl) setPeer(peerId int, peerInfo gc.PeerRPCProxy) {
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
	HostID               int
	HostURL              string
	Logger               observability.Logger
	RpcDialTimeout       time.Duration
	RpcReconnectDuration time.Duration
	RpcRequestTimeout    time.Duration
	StaticClusterMembers []gc.ClusterMember
}

func NewInternalRPC(params NewRPCImplParams) (*RPCProxyImpl, error) {
	r := RPCProxyImpl{
		hostID:               params.HostID,
		hostURL:              params.HostURL,
		logger:               params.Logger,
		stop:                 nil,
		accessible:           true,
		peers:                make(map[int]gc.PeerRPCProxy),
		rpcDialTimeout:       params.RpcDialTimeout,
		rpcReconnectDuration: params.RpcReconnectDuration,
		rpcRequestTimeout:    params.RpcRequestTimeout,
	}

	// if len(params.StaticClusterMembers) > 0 {
	// 	for _, peer := range params.StaticClusterMembers {
	// 		r.peers[peer.ID] = gc.PeerRPCProxy{
	// 			Conn: nil,
	// 			URL:  peer.RpcUrl,
	// 		}
	// 	}
	// }

	return &r, nil
}

func Difference(current, newConfig map[int]gc.PeerRPCProxy) (toBeRemoved, toBeAdded map[int]gc.PeerRPCProxy) {
	toBeRemoved = make(map[int]gc.PeerRPCProxy)
	toBeAdded = make(map[int]gc.PeerRPCProxy)

	for k, v := range current {
		if _, exists := newConfig[k]; !exists {
			toBeRemoved[k] = v
		}
	}

	for k, v := range newConfig {
		if _, exists := current[k]; !exists {
			toBeAdded[k] = v
		}
	}

	return toBeRemoved, toBeAdded
}

func (r *RPCProxyImpl) InformMemberChange(ctx context.Context, members map[int]gc.ClusterMember) {
	newData := map[int]gc.PeerRPCProxy{}
	for _, mem := range members {
		newData[mem.ID] = gc.PeerRPCProxy{Conn: nil, URL: mem.RpcUrl}
	}

	toBeRemoved, toBeAdded := Difference(r.peers, newData)

	for peerId, peer := range toBeRemoved {
		err := r.disconnectToPeer(ctx, peerId)
		if err != nil {
			r.log().ErrorContext(
				ctx, "InformMemberChange_DisconnectToPeer", err,
				"peer", peer,
				"peerId", peerId,
			)
		}
	}

	for peerId, peer := range toBeAdded {
		r.peers[peerId] = peer
		err := r.connectToPeer(ctx, peerId, 0, time.Second)
		if err != nil {
			r.log().ErrorContext(
				ctx, "InformMemberChange_ConnectToPeer", err,
				"peer", peer,
				"peerId", peerId,
			)
		}
	}
}

func (r *RPCProxyImpl) SetBrain(brain RaftBrain) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.brain = brain
}

func (r *RPCProxyImpl) ConnectToNewPeer(ctx context.Context, peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	r.setPeer(peerID, gc.PeerRPCProxy{Conn: nil, URL: peerURL})
	return r.connectToPeer(ctx, peerID, retry, retryDelay)
}

func (r *RPCProxyImpl) DisconnectToAllPeers(ctx context.Context) {
	for id := range r.peers {
		r.disconnectToPeer(ctx, id)
	}

	r.peers = map[int]gc.PeerRPCProxy{}
}

func (r *RPCProxyImpl) disconnectToPeer(ctx context.Context, peerID int) error {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return err
	}

	r.log().DebugContext(ctx, "disconnectToPeer", "peerId", peerID)

	if err := peer.Conn.Close(); err != nil {
		r.log().ErrorContext(ctx, "disconnectToPeer_close", err)
	}

	r.setPeer(peerID, gc.PeerRPCProxy{Conn: nil, URL: peer.URL})

	return nil
}

func (r *RPCProxyImpl) connectToPeer(ctx context.Context, peerID int, retry int, retryDelay time.Duration) error {
	start := time.Now()
	defer func() {
		r.log().InfoContext(
			ctx, "connectToPeer",
			"duration(ms)", time.Since(start).Milliseconds(),
			"peerID", peerID,
		)
	}()

	// an connection exist, test connection
	peer, err := r.getPeer(peerID)
	if err == nil && peer.Conn != nil {
		res, err := r.pingWithNoRetry(ctx, peerID, &r.rpcRequestTimeout)
		if err == nil {
			if res.ID == peerID {
				return nil
			}
		}
	}

	// non connection exist or connection test failed
	r.setPeer(peerID, gc.PeerRPCProxy{
		Conn: nil,
		URL:  peer.URL,
	})

	for i := 0; i < retry; i++ {
		conn, err := net.DialTimeout("tcp", peer.URL, r.rpcDialTimeout)
		// client, err := rpc.Dial("tcp", peerURL)
		if err != nil {
			r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: can't connect to %s ", peer.URL), err)

			time.Sleep(retryDelay)
		} else {
			r.log().InfoContext(ctx, fmt.Sprintf("ConnectToPeer: connect to %s successfully", peer.URL), err)

			r.setPeer(peerID, gc.PeerRPCProxy{
				Conn: rpc.NewClient(conn),
				URL:  peer.URL,
			})

			res, err := r.pingWithNoRetry(ctx, peerID, &r.rpcRequestTimeout)
			if err != nil {
				r.log().ErrorContext(ctx, fmt.Sprintf("ConnectToPeer: cannot ping %s", peer.URL), err)
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
		r.log().FatalContext(ctx, "initRPCProxy: Listener error", "error", err.Error())

		return err
	}

	r.listener = listener

	r.log().InfoContext(ctx, "initRPCProxy: finished register node")

	return nil
}

func (r *RPCProxyImpl) waitForConnection(ctx context.Context) {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			r.log().ErrorContext(ctx, "RPCProxy: listener error", err)

			break
		} else {
			go r.rpcServer.ServeConn(conn)
		}

	}

	r.log().InfoContext(ctx, "RPCProxy waitForConnection stop")
}

func (r *RPCProxyImpl) stopConnections(ctx context.Context) {
	err := r.listener.Close()
	if err != nil {
		r.log().ErrorContext(ctx, "initRPCProxy: Listener error", err)
	}
}

// this function periodically attempt to connecting crashed peers
func (r *RPCProxyImpl) reconnectPeer(ctx context.Context) {
	ticker := time.NewTicker(r.rpcReconnectDuration)
	for {
		select {
		case <-ctx.Done():
			r.log().InfoContext(ctx, "reconnectPeer stop")
			return
		case <-ticker.C:
			for peerId, peer := range r.peers {
				if peer.Conn == nil {
					ctx := context.Background()
					r.log().DebugContext(ctx, "reconnectPeer_connectToPeer", "peerId", peerId)
					go r.connectToPeer(ctx, peerId, 1, r.rpcDialTimeout)
				}
			}
		}
	}
}

func (r *RPCProxyImpl) Start(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Start RPC Server")
	defer span.End()

	if err := r.initServer(ctx, r.hostURL); err != nil {
		r.log().FatalContext(ctx, "Start RPC Proxy", "error", err.Error())
	}

	ctx, cancel := context.WithCancel(ctx)

	r.stop = cancel

	go r.waitForMemberChange(ctx, r.brain.GetNewMembersChannel())
	go r.waitForConnection(ctx)
	go r.reconnectPeer(ctx)
}

// add or remove member (node) from the cluster
func (r *RPCProxyImpl) waitForMemberChange(ctx context.Context, channel <-chan common.ClusterMemberChange) {
	for {
		select {
		case <-ctx.Done():
			r.logger.InfoContext(ctx, "waitForMemberChange stop")
			return
		case member := <-channel:
			if r.hostID != member.ID {
				if member.Add {
					r.logger.Info("connect new member", "member", member)
					r.setPeerIfNew(member.ID, gc.PeerRPCProxy{Conn: nil, URL: member.RpcUrl})
					r.connectToPeer(ctx, member.ID, 5, r.rpcDialTimeout)
				} else {
					r.logger.Info("disconnect new member", "member", member)
					r.disconnectToPeer(ctx, member.ID)
				}
			}
		}
	}
}

var (
	ErrPeerIdDoesNotExist      = errors.New("rpc peer id does not exist")
	ErrRpcPeerConnectionIsNull = errors.New("rpc peer connection is nil")
	ErrRpcTimeout              = errors.New("rpc call take too long")
)

func (r *RPCProxyImpl) callWithoutTimeout(ctx context.Context, peerID int, serviceMethod string, args any, reply any) (err error) {
	var peer gc.PeerRPCProxy

	peer, err = r.getPeer(peerID)
	if err != nil {
		return err
	}

	if peer.Conn != nil {
		return peer.Conn.Call(serviceMethod, args, reply)
	}
	return ErrRpcPeerConnectionIsNull
}

// internal use, to test connection after dial,
// no retry
func (r *RPCProxyImpl) pingWithNoRetry(ctx context.Context, peerID int, timeout *time.Duration) (reply gc.PingResponse, err error) {
	peer, err := r.getPeer(peerID)
	if err != nil {
		return reply, err
	}

	reply = gc.PingResponse{}
	serviceMethod := "RPCProxyImpl.Ping"
	args := gc.PingRequest{ID: r.hostID}

	if peer.Conn != nil {
		call := peer.Conn.Go(serviceMethod, args, &reply, nil)
		select {
		case <-time.After(*timeout):
			r.disconnectToPeer(ctx, peerID)
			return reply, ErrRpcTimeout
		case resp := <-call.Done:
			if resp != nil && resp.Error != nil {
				r.disconnectToPeer(ctx, peerID)
				return reply, resp.Error
			}
		}

		return reply, nil
	}
	return reply, ErrRpcPeerConnectionIsNull
}

// TODO: bring timeout into context
func (r *RPCProxyImpl) callWithTimeout(ctx context.Context, peerID int, serviceMethod string, args any, reply any, timeout time.Duration) (err error) {
	var peer gc.PeerRPCProxy

	peer, err = r.getPeer(peerID)
	if err != nil {
		return err
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
	r.stop()
	r.stopConnections(context.Background())
}
