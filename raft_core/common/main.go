package common

import (
	gc "khanh/raft-go/common"
)

type AppendEntriesInput struct {
	Term         int      // leader’s term
	LeaderID     int      // so follower can redirect clients
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []gc.Log // log entries to store ()
	LeaderCommit int      // leader’s commitIndex

	Trace *gc.RequestTraceInfo // this will be set at RPC Proxy
}

type AppendEntriesOutput struct {
	Term    int    // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
	Message string // for debuging purpose
	NodeID  int    // id of the responder
}

type RequestVoteInput struct {
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)

	Trace *gc.RequestTraceInfo // this will be set at RPC Proxy
}

type RequestVoteOutput struct {
	Term        int    // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
	Message     string // for debuging purpose
	NodeID      int    // id of the responder
}

type ToVotingMemberInput struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
	TraceState string
}

type ClusterMemberChange struct {
	gc.ClusterMember
	Add   bool
	Reset bool
}
