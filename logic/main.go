package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"math"
	"time"

	"github.com/rs/zerolog"
)

type RaftState string

const (
	StateFollower  RaftState = "follower"
	StateCandidate RaftState = "candidate"
	StateLeader    RaftState = "leader"
)

func (s RaftState) String() string {
	return string(s)
}

type RaftBrainImpl struct {
	// TODO: add mutex
	logger            *zerolog.Logger
	DB                persistance.Persistence
	Peers             []PeerInfo
	State             RaftState
	ID                int
	StateMachine      SimpleStateMachine
	ElectionTimeOut   *time.Timer
	HeartBeatTimeOut  *time.Timer
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
	NextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex map[int]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type SimpleStateMachine interface {
	Process(command any) (result any, err error)
}

type RPCProxy interface {
	SendAppendEntries(peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendPing(peerId int, timeout *time.Duration) (err error)
}

type PeerInfo struct {
	ID  int
	URL string
}

type NewRaftBrainParams struct {
	ID                int
	Peers             []PeerInfo
	DataFileName      string
	MinRandomDuration int64
	MaxRandomDuration int64
	Log               *zerolog.Logger
}

func NewRaftBrain(params NewRaftBrainParams) (*RaftBrainImpl, error) {
	n := &RaftBrainImpl{
		ID:           params.ID,
		State:        StateFollower,
		VotedFor:     0,
		Quorum:       int(math.Ceil(float64(len(params.Peers)) / 2.0)),
		DB:           persistance.NewPersistence(params.DataFileName),
		StateMachine: common.NewKeyValueStateMachine(),

		MinRandomDuration: params.MinRandomDuration,
		MaxRandomDuration: params.MaxRandomDuration,
		logger:            params.Log,
		Peers:             params.Peers,
		NextIndex:         make(map[int]int),
		MatchIndex:        make(map[int]int),
	}

	err := n.Rehydrate()
	if err != nil {
		return n, err
	}

	n.applyLog()

	for _, peer := range n.Peers {
		n.NextIndex[peer.ID] = len(n.Logs) + 1
		n.MatchIndex[peer.ID] = 0
	}

	return n, nil
}

func (n *RaftBrainImpl) Start() {
	n.resetElectionTimeout()
	n.resetHeartBeatTimeout()

	go n.loop()
}

func (n *RaftBrainImpl) log() *zerolog.Logger {
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

func (n *RaftBrainImpl) resetElectionTimeout() {
	randomElectionTimeOut := time.Duration(common.RandInt(n.MinRandomDuration, n.MaxRandomDuration)) * time.Millisecond
	randomElectionTimeOut *= 2
	n.log().Info().Interface("seconds", randomElectionTimeOut.Seconds()).Msg("resetElectionTimeout")
	if n.ElectionTimeOut == nil {
		n.ElectionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.ElectionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *RaftBrainImpl) resetHeartBeatTimeout() {
	randomHeartBeatTimeout := time.Duration(common.RandInt(n.MinRandomDuration, n.MaxRandomDuration)) * time.Millisecond
	n.log().Info().Interface("seconds", randomHeartBeatTimeout.Seconds()).Msg("resetHeartBeatTimeout")
	if n.HeartBeatTimeOut == nil {
		n.HeartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.HeartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}
