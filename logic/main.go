package logic

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"math"
	"sync"
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

type SessionManager interface{}

type RaftBrainImpl struct {
	lock sync.RWMutex
	// TODO: add mutex
	logger            *zerolog.Logger
	DB                persistance.Persistence
	Peers             []common.PeerInfo
	State             RaftState
	ID                int
	StateMachine      SimpleStateMachine
	ElectionTimeOut   *time.Timer
	HeartBeatTimeOut  *time.Timer
	Quorum            int
	MinRandomDuration int64
	MaxRandomDuration int64
	RpcProxy          RPCProxy
	Session           SessionManager
	ARM               AsyncResponseManager
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

type AsyncResponseItem struct {
	msg       chan any
	createdAt time.Time
}

type AsyncResponseIndex struct {
	LogIndex int
}

type AsyncResponseManager struct {
	m    map[AsyncResponseIndex]AsyncResponseItem // client id -> response
	size int
	l    sync.Mutex
}

func NewAsyncResponseManager(size int) AsyncResponseManager {
	return AsyncResponseManager{m: map[AsyncResponseIndex]AsyncResponseItem{}, size: size}
}

func (a *AsyncResponseManager) GetAndDeleteResponse(logIndex int) (any, error) {
	a.l.Lock()
	defer a.l.Unlock()

	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if !ok {
		return nil, fmt.Errorf("there are no cached respose for log index: %d", logIndex)
	}

	defer delete(a.m, index)

	return a.m[index], nil
}

func (a *AsyncResponseManager) Register(logIndex int) error {
	a.l.Lock()
	defer a.l.Unlock()

	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if ok {
		return fmt.Errorf("already registered log index: %d", logIndex)
	}

	a.m[index] = AsyncResponseItem{
		msg:       make(chan any),
		createdAt: time.Now(),
	}

	return nil
}

func (a *AsyncResponseManager) PutResponse(logIndex int, msg any, resErr error) error {
	a.l.Lock()
	defer a.l.Unlock()

	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if !ok {
		// return fmt.Errorf("register log index: %d first", logIndex)
		return nil
	}

	select {
	case a.m[index].msg <- msg:
	default:
		return fmt.Errorf("channel log index: %d is not empty", logIndex)
	}

	return nil
}

// blocking call
func (a *AsyncResponseManager) TakeResponse(logIndex int, timeout time.Duration) (any, error) {
	a.l.Lock()
	defer a.l.Unlock()

	index := AsyncResponseIndex{logIndex}

	item, ok := a.m[index]
	if !ok {
		return nil, fmt.Errorf("register log index: %d first", logIndex)
	}

	select {
	case res := <-item.msg:
		close(item.msg)
		delete(a.m, index)
		return res, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout error: can't get messsage")
	}
}

type LogAppliedEvent struct {
	SequenceNum int
	Response    any
	Err         error
}

type SimpleStateMachine interface {
	Process(clientID int, sequenceNum int, commandIn any, logIndex int) (result any, err error)
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
	Peers             []common.PeerInfo
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
		ARM:          NewAsyncResponseManager(100),

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
		if peer.ID != n.ID {
			n.NextIndex[peer.ID] = len(n.Logs) + 1
			n.MatchIndex[peer.ID] = 0
		}
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
