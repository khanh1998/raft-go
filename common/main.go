package common

import (
	"context"
	"net/rpc"

	"go.opentelemetry.io/otel/trace"
)

const NoOperation = "NO-OP"

type AppendEntriesInput struct {
	Term         int   // leader’s term
	LeaderID     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex

	Trace *RequestTraceInfo // this will be set at RPC Proxy
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

	Trace *RequestTraceInfo // this will be set at RPC Proxy
}

// this struct carries need info so OpenTelemery can trace the requests between nodes.
type RequestTraceInfo struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
	TraceState string
}

func NewRequestTraceInfo(traceId string, spanId string, flags byte, state string) RequestTraceInfo {
	return RequestTraceInfo{
		TraceID:    traceId,
		SpanID:     spanId,
		TraceFlags: flags,
		TraceState: state,
	}
}

func (r *RequestTraceInfo) Context() (context.Context, error) {
	traceID, err := trace.TraceIDFromHex(r.TraceID)
	if err != nil {
		return nil, err
	}

	spanID, err := trace.SpanIDFromHex(r.SpanID)
	if err != nil {
		return nil, err
	}

	traceFlags := trace.TraceFlags(r.TraceFlags)

	traceState, err := trace.ParseTraceState(r.TraceState)
	if err != nil {
		return nil, err
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		Remote:     true,
		TraceFlags: traceFlags,
		TraceState: traceState,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	return ctx, nil
}

type RequestVoteOutput struct {
	Term        int    // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
	Message     string // for debuging purpose
	NodeID      int    // id of the responder
}

type RaftState string

const (
	StateFollower   RaftState = "follower"
	StateCandidate  RaftState = "candidate"
	StateLeader     RaftState = "leader"
	StateCatchingUp RaftState = "catching-up" // new node is catching up with current leader of the cluster, it can't vote
	StateRemoved    RaftState = "removed"     // got removed from cluster
)

func (s RaftState) String() string {
	return string(s)
}

type GetStatusResponse struct {
	ID       int
	State    RaftState
	Term     int
	LeaderId int
}

type PeerRPCProxy struct {
	Conn *rpc.Client
	URL  string
}

type PingResponse struct {
	ID       int
	LeaderId int
	RpcUrl   string
	Message  string
	State    RaftState
	Term     int
}

type ToVotingMemberInput struct {
	TraceID    string
	SpanID     string
	TraceFlags byte
	TraceState string
}
