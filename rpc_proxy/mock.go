package rpc_proxy

import (
	"khanh/raft-go/common"
	"time"
)

type RPCProxy interface {
	SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendPing(peerId int, timeout *time.Duration) (err error)
}

type RPCProxyMock struct {
	appendEntries map[int]common.AppendEntriesOutput
	requestVote   map[int]common.RequestVoteOutput
}

func (r RPCProxyMock) SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error) {
	return r.appendEntries[peerId], nil
}

func (r RPCProxyMock) SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error) {
	return r.requestVote[peerId], nil
}

func (r RPCProxyMock) SendPing(peerId int, timeout *time.Duration) (err error) {
	return
}
