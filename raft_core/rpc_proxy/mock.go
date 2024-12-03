package rpc_proxy

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/common"
	"time"
)

type RPCProxy interface {
	SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendPing(ctx context.Context, peerId int, timeout *time.Duration) (err error)
}

type RPCProxyMock struct {
	AppendEntries   map[int]common.AppendEntriesOutput
	RequestVote     map[int]common.RequestVoteOutput
	Ping            map[int]gc.PingResponse
	InstallSnapshot map[int]common.InstallSnapshotOutput
	Logger          observability.Logger
}

func (r RPCProxyMock) SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	res, ok := r.AppendEntries[peerId]
	if ok {
		return res, nil
	}
	time.Sleep(*timeout)
	return output, ErrRpcTimeout
}

func (r RPCProxyMock) SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	res, ok := r.RequestVote[peerId]
	if ok {
		return res, nil
	}
	time.Sleep(*timeout)
	return output, ErrRpcTimeout
}

func (r RPCProxyMock) SendPing(ctx context.Context, peerId int, timeout *time.Duration) (res gc.PingResponse, err error) {
	res, ok := r.Ping[peerId]
	if ok {
		return res, nil
	}
	time.Sleep(*timeout)
	return res, ErrRpcTimeout
}

func (r RPCProxyMock) ConnectToNewPeer(ctx context.Context, peerID int, peerURL string, retry int, retryDelay time.Duration) error {
	return nil
}

func (r RPCProxyMock) SendToVotingMember(ctx context.Context, peerId int, timeout *time.Duration) (err error) {
	return nil
}

func (r RPCProxyMock) SendInstallSnapshot(ctx context.Context, peerId int, timeout *time.Duration, input common.InstallSnapshotInput) (output common.InstallSnapshotOutput, err error) {
	r.Logger.Info("SendInstallSnapshot", "peerId", peerId, "input", input)
	res, ok := r.InstallSnapshot[peerId]
	if ok {
		return res, nil
	}
	time.Sleep(*timeout)
	return res, ErrRpcTimeout
}
