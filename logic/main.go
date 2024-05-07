package logic

import (
	"fmt"
	"khanh/raft-go/common"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type SessionManager interface{}

type RaftBrainImpl struct {
	// TODO: add mutex
	logger            *zerolog.Logger
	DB                Persistence
	Peers             []common.PeerInfo
	State             common.RaftState
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
	Stop              chan struct{}
	AddServerLock     sync.Mutex
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
	NextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) // TODO: data race
	MatchIndex map[int]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically) // TODO: data race
}

type AsyncResponseItem struct {
	Response any
	Err      error
}
type AsyncResponse struct {
	msg       chan AsyncResponseItem
	createdAt time.Time
}

type AsyncResponseIndex struct {
	LogIndex int
}

type AsyncResponseManager struct {
	m    map[AsyncResponseIndex]AsyncResponse // client id -> response
	size int
	l    sync.Mutex
}

func NewAsyncResponseManager(size int) AsyncResponseManager {
	return AsyncResponseManager{m: map[AsyncResponseIndex]AsyncResponse{}, size: size}
}

func (a *AsyncResponseManager) Register(logIndex int) error {
	a.l.Lock()
	defer a.l.Unlock()

	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if ok {
		return fmt.Errorf("already registered log index: %d", logIndex)
	}

	a.m[index] = AsyncResponse{
		msg:       make(chan AsyncResponseItem, 1),
		createdAt: time.Now(),
	}

	log.Info().
		Interface("data", a.m[index]).
		Interface("index", logIndex).
		Interface("capacity", cap(a.m[index].msg)).
		Msg("Register")

	return nil
}

func (a *AsyncResponseManager) PutResponse(logIndex int, msg any, resErr error, timeout time.Duration) error {
	index := AsyncResponseIndex{logIndex}

	_, ok := a.m[index]
	if !ok {
		return fmt.Errorf("register log index: %d first", logIndex)
	}

	select {
	case a.m[index].msg <- AsyncResponseItem{Response: msg, Err: resErr}:
		log.Info().Int("log index", logIndex).Interface("msg", msg).Interface("err", resErr).Msg("PutResponse")
	case <-time.After(timeout):
		return fmt.Errorf("channel log index: %d is not empty", logIndex)
	}

	return nil
}

// blocking call
func (a *AsyncResponseManager) TakeResponse(logIndex int, timeout time.Duration) (any, error) {
	index := AsyncResponseIndex{logIndex}
	item, ok := a.m[index]
	if !ok {
		return nil, fmt.Errorf("register log index: %d first", logIndex)
	}

	select {
	case res := <-item.msg:
		close(item.msg)
		delete(a.m, index)
		return res.Response, res.Err
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

	ConnectToNewPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error
}

type Persistence interface {
	AppendLog(data map[string]string) error
	ReadNewestLog(keys []string) (map[string]string, error)
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
	DB                Persistence
	StateMachine      SimpleStateMachine
}

func NewRaftBrain(params NewRaftBrainParams) (*RaftBrainImpl, error) {
	n := &RaftBrainImpl{
		ID:           params.ID,
		State:        common.StateFollower,
		VotedFor:     0,
		DB:           params.DB,
		StateMachine: params.StateMachine,
		ARM:          NewAsyncResponseManager(100),
		Stop:         make(chan struct{}),

		MinRandomDuration: params.MinRandomDuration,
		MaxRandomDuration: params.MaxRandomDuration,
		logger:            params.Log,
		Peers:             []common.PeerInfo{},
		NextIndex:         make(map[int]int),
		MatchIndex:        make(map[int]int),
	}

	err := n.Rehydrate()
	if err != nil {
		return n, err
	}

	n.applyLog()

	for _, peer := range params.Peers {
		if peer.ID != n.ID {
			n.Peers = append(n.Peers, peer)
		}
	}

	n.Quorum = int(math.Ceil(float64(len(n.Peers)) / 2.0))

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
		Int("RB_ID", n.ID).
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
	randomElectionTimeOut := time.Duration(common.RandInt(n.MinRandomDuration*10, n.MaxRandomDuration*10)) * time.Millisecond
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

func (n *RaftBrainImpl) GetInfo() common.GetStatusResponse {
	return common.GetStatusResponse{
		ID:    n.ID,
		State: n.State,
		Term:  n.CurrentTerm,
	}
}
