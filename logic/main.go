package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"math"
	"time"

	"github.com/rs/zerolog"
)

type ServerState string

const (
	StateFollower  ServerState = "follower"
	StateCandidate ServerState = "candidate"
	StateLeader    ServerState = "leader"
)

func (s ServerState) String() string {
	return string(s)
}

type NodeImpl struct {
	logger            *zerolog.Logger
	DB                persistance.Persistence
	PeerURLs          []string
	State             ServerState
	ID                int
	StateMachine      common.StateMachine
	ElectionTimeOut   *time.Timer
	HeartBeatTimeOut  *time.Timer
	stop              chan struct{}
	Quorum            int
	MinRandomDuration int64
	MaxRandomDuration int64
	RpcProxy          RPCProxy
	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs
	CurrentTerm int          // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int          // candidateId that received vote in current term (or null if none)
	Logs        []common.Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	CommitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// Reinitialized after election
	NextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type RPCProxy interface {
	SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendPing(peerId int, timeout *time.Duration) (err error)
}

type NewNodeParams struct {
	ID                int
	PeerRpcURLs       []string
	RpcHostURL        string
	RestApiHostURL    string
	DataFileName      string
	MinRandomDuration int64
	MaxRandomDuration int64
	Log               *zerolog.Logger
}

func NewNode(params NewNodeParams) (*NodeImpl, error) {
	n := &NodeImpl{
		ID:           params.ID,
		State:        StateFollower,
		VotedFor:     0,
		Quorum:       int(math.Ceil(float64(len(params.PeerRpcURLs)) / 2.0)),
		DB:           persistance.NewPersistence(params.DataFileName),
		stop:         make(chan struct{}),
		StateMachine: common.NewStateMachine(),

		MinRandomDuration: params.MinRandomDuration,
		MaxRandomDuration: params.MaxRandomDuration,
		logger:            params.Log,
		PeerURLs:          params.PeerRpcURLs,
	}

	// n.initRPC(params.RpcHostURL)
	// n.initApi(params.RestApiHostURL)

	err := n.Rehydrate()
	if err != nil {
		return n, err
	}

	n.applyLog()

	for i := 0; i < len(params.PeerRpcURLs); i++ {
		n.NextIndex = append(n.NextIndex, len(n.Logs)+1)
		n.MatchIndex = append(n.MatchIndex, 0)
	}

	// n.ConnectToPeers()

	go n.loop()
	return n, nil
}

func (n *NodeImpl) log() *zerolog.Logger {
	sub := n.logger.With().
		Str("state", n.State.String()).
		Int("voted for", n.VotedFor).
		Int("term", n.CurrentTerm).
		Int("commitIndex", n.CommitIndex).
		Int("lastApplied", n.LastApplied).
		Interface("nextIndex", n.NextIndex).
		Interface("matchIndex", n.MatchIndex).
		Logger()
	return &sub
}

func (n NodeImpl) Stop() chan struct{} {
	return n.stop
}

func (n *NodeImpl) resetElectionTimeout() {
	randomElectionTimeOut := time.Duration(common.RandInt(n.MinRandomDuration, n.MaxRandomDuration)) * time.Millisecond
	randomElectionTimeOut *= 5
	n.log().Info().Interface("seconds", randomElectionTimeOut.Seconds()).Msg("resetElectionTimeout")
	if n.ElectionTimeOut == nil {
		n.ElectionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.ElectionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *NodeImpl) resetHeartBeatTimeout() {
	randomHeartBeatTimeout := time.Duration(common.RandInt(n.MinRandomDuration, n.MaxRandomDuration)) * time.Millisecond
	n.log().Info().Interface("seconds", randomHeartBeatTimeout.Seconds()).Msg("resetHeartBeatTimeout")
	if n.HeartBeatTimeOut == nil {
		n.HeartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.HeartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}
