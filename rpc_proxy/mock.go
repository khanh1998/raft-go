package rpc_proxy

import (
	"context"
	"khanh/raft-go/common"
	"time"
)

type RPCProxy interface {
	SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendPing(ctx context.Context, peerId int, timeout *time.Duration) (err error)
}

type RPCProxyMock struct {
	appendEntries map[int]common.AppendEntriesOutput
	requestVote   map[int]common.RequestVoteOutput
}

func (r RPCProxyMock) SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	return r.appendEntries[peerId], nil
}

func (r RPCProxyMock) SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	return r.requestVote[peerId], nil
}

func (r RPCProxyMock) SendPing(ctx context.Context, peerId int, timeout *time.Duration) (res common.PingResponse, err error) {
	return
}

func (r RPCProxyMock) ConnectToNewPeer(ctx context.Context, eerID int, peerURL string, retry int, retryDelay time.Duration) error {
	return nil
}

func (r RPCProxyMock) SendToVotingMember(ctx context.Context, peerId int, timeout *time.Duration) (err error) {
	return nil
}

func (r RPCProxyMock) AddServer(ctx context.Context, member common.ClusterMember) {}

func (r RPCProxyMock) SendInstallSnapshot(ctx context.Context, peerId int, timeout *time.Duration, input common.InstallSnapshotInput) (output common.InstallSnapshotOutput, err error) {
	return
}
