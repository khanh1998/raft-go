package common

import (
	gc "khanh/raft-go/common"
)

type BeginSnapshotResponse struct {
	LastLogTerm  int
	LastLogIndex int
}

type SnapshotNotification struct {
	LastConfig map[int]gc.ClusterMember // cluster members
	LastTerm   int
	LastIndex  int
}

type InstallSnapshotInput struct {
	Term      int
	LeaderId  int
	LastIndex int

	LastTerm   int
	LastConfig []gc.ClusterMember

	FileName string
	Offset   int64
	Data     []byte

	Done bool

	Trace *gc.RequestTraceInfo // this will be set at RPC Proxy
}

type InstallSnapshotOutput struct {
	Term    int
	Success bool   // follower response true to continue installing snapshot, false to stop
	Message string // for debugging purpose
	NodeID  int    // id of the responder
}
