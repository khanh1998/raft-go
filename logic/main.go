package logic

import (
	"errors"
	"khanh/raft-go/common"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type SessionManager interface{}
type MembershipManager interface {
	Process(command any) error
	AddServer(input common.AddServerInput) error
}

type RaftBrainImpl struct {
	logger                    *zerolog.Logger
	DB                        Persistence
	Members                   []common.ClusterMember
	nextMemberId              int
	State                     common.RaftState
	ID                        int
	LeaderID                  int
	StateMachine              SimpleStateMachine
	ElectionTimeOut           *time.Timer
	HeartBeatTimeOut          *time.Timer
	HeartBeatTimeOutMin       int64 // millisecond
	HeartBeatTimeOutMax       int64 // millisecond
	ElectionTimeOutMin        int64 // millisecond
	ElectionTimeOutMax        int64 // millisecond
	RpcProxy                  RPCProxy
	Session                   SessionManager
	ARM                       AsyncResponseManager
	Stop                      chan struct{}
	newMembers                chan common.ClusterMemberChange
	InOutLock                 sync.RWMutex // lock for inbound and outbound RPC methods and for client interaction
	ChangeMemberLock          sync.Mutex   // lock for adding or removing a member from the cluster
	lastHeartbeatReceivedTime time.Time
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
	SendPing(peerId int, timeout *time.Duration) (res common.PingResponse, err error)

	ConnectToNewPeer(peerID int, peerURL string, retry int, retryDelay time.Duration) error
	SendToVotingMember(peerId int, timeout *time.Duration) (err error)
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
	ID   int
	Mode common.ClusterMode
	// list of member servers for STATIC cluster mode.
	// if cluster mode is DYNAMIC, list contains only one member - is the first node of cluster.
	Members             []common.ClusterMember
	DataFileName        string
	HeartBeatTimeOutMin int64
	HeartBeatTimeOutMax int64
	ElectionTimeOutMin  int64
	ElectionTimeOutMax  int64
	Log                 *zerolog.Logger
	DB                  Persistence
	StateMachine        SimpleStateMachine
	CachingUp           bool // will be ignored if the cluster mode is STATIC
}

func NewRaftBrain(params NewRaftBrainParams) (*RaftBrainImpl, error) {
	n := &RaftBrainImpl{
		ID: params.ID,
		State: func() common.RaftState {
			if params.CachingUp {
				return common.StateCatchingUp
			}
			return common.StateFollower
		}(),
		VotedFor:     0,
		DB:           params.DB,
		StateMachine: params.StateMachine,
		ARM:          NewAsyncResponseManager(100),
		Stop:         make(chan struct{}),
		newMembers:   make(chan common.ClusterMemberChange, 10),
		nextMemberId: params.ID + 1,

		HeartBeatTimeOutMin: params.HeartBeatTimeOutMin,
		HeartBeatTimeOutMax: params.HeartBeatTimeOutMax,
		ElectionTimeOutMin:  params.ElectionTimeOutMin,
		ElectionTimeOutMax:  params.ElectionTimeOutMax,

		lastHeartbeatReceivedTime: time.Now(),

		logger:     params.Log,
		Members:    []common.ClusterMember{},
		NextIndex:  make(map[int]int),
		MatchIndex: make(map[int]int),
	}

	err := n.restoreRaftStateFromFile()
	if err != nil {
		return n, err
	}

	if params.Mode == common.Dynamic {
		if params.CachingUp {
			// if the new node is in catching up mode,
			// we ignore all member configurations, because it hasn't been a part of cluster yet.
			if len(params.Members) > 0 {
				return nil, errors.New("in catching up mode, we don't pass member list as parameter")
			}
		} else {
			if len(params.Members) != 1 {
				return nil, errors.New("in dynamic cluster, initially there is one server")
			}

			// if this is the first node of cluster and the first time the node get boosted up,
			// initialy add members to the cluster. otherwise, it's already in the log.
			if len(n.Logs) == 0 {
				mem := params.Members[0]

				n.appendLog(common.Log{
					Term:        1,
					ClientID:    0,
					SequenceNum: 0,
					Command:     common.ComposeAddServerCommand(mem.ID, mem.HttpUrl, mem.RpcUrl),
				})
			} else {
				n.restoreClusterMemberInfoFromLogs()
			}
		}
	}

	if params.Mode == common.Static {
		if len(params.Members) == 0 {
			return nil, errors.New("in static cluster, you need to pass the member list of cluster")
		}

		for _, mem := range params.Members {
			n.addMember(mem.ID, mem.HttpUrl, mem.RpcUrl)
		}
	}

	n.applyLog()

	return n, nil
}

func (n *RaftBrainImpl) Start() {
	n.resetElectionTimeout()
	n.resetHeartBeatTimeout()

	go n.loop()

	n.log().Info().
		Interface("members", n.Members).
		Msg("Brain start")
}

func (n *RaftBrainImpl) log() *zerolog.Logger {
	// data race
	sub := n.logger.With().
		Int("id", n.ID).
		Str("state", n.State.String()).
		Int("votedFor", n.VotedFor).
		Int("leaderId", n.VotedFor).
		Int("term", n.CurrentTerm).
		Int("commitIndex", n.CommitIndex).
		Int("lastApplied", n.LastApplied).
		Interface("nextIndex", n.NextIndex).
		Interface("matchIndex", n.MatchIndex).
		Logger()
	return &sub
}

func (n *RaftBrainImpl) GetInfo() common.GetStatusResponse {
	return common.GetStatusResponse{
		ID:    n.ID,
		State: n.State,
		Term:  n.CurrentTerm,
	}
}
