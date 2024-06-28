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
	dataFolder                string
	logger                    *zerolog.Logger
	db                        Persistence
	members                   []common.ClusterMember
	nextMemberId              int
	state                     common.RaftState
	id                        int
	leaderID                  int
	stateMachine              SimpleStateMachine
	electionTimeOut           *time.Timer
	heartBeatTimeOut          *time.Timer
	heartBeatTimeOutMin       int64 // millisecond
	heartBeatTimeOutMax       int64 // millisecond
	electionTimeOutMin        int64 // millisecond
	electionTimeOutMax        int64 // millisecond
	rpcProxy                  RPCProxy
	arm                       AsyncResponseManager
	stop                      chan struct{}
	newMembers                chan common.ClusterMemberChange
	inOutLock                 sync.RWMutex // this lock help to process requests in sequential order. requests are processed one after the other, not concurrently.
	changeMemberLock          sync.Mutex   // lock for adding or removing a member from the cluster
	dataLock                  sync.RWMutex // lock for reading or modifying internal data of the brain (consensus module)
	lastHeartbeatReceivedTime time.Time
	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs
	currentTerm int          // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int          // candidateId that received vote in current term (or null if none)
	logs        []common.Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// Reinitialized after election
	nextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
	Reset() error
	Process(clientID int, sequenceNum int, commandIn any, logIndex int) (result any, err error)
	GetBase() (lastIndex int, lastTerm int)
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
	HeartBeatTimeOutMin int64
	HeartBeatTimeOutMax int64
	ElectionTimeOutMin  int64
	ElectionTimeOutMax  int64
	Logger              *zerolog.Logger
	DB                  Persistence // help to persist raft server's state to file
	CachingUp           bool        // will be ignored if the cluster mode is STATIC
}

func NewRaftBrain(params NewRaftBrainParams) (*RaftBrainImpl, error) {
	n := &RaftBrainImpl{
		id: params.ID,
		state: func() common.RaftState {
			if params.CachingUp {
				return common.StateCatchingUp
			}
			return common.StateFollower
		}(),
		votedFor:     0,
		db:           params.DB,
		arm:          NewAsyncResponseManager(100),
		stop:         make(chan struct{}),
		newMembers:   make(chan common.ClusterMemberChange, 10),
		nextMemberId: params.ID + 1,

		heartBeatTimeOutMin: params.HeartBeatTimeOutMin,
		heartBeatTimeOutMax: params.HeartBeatTimeOutMax,
		electionTimeOutMin:  params.ElectionTimeOutMin,
		electionTimeOutMax:  params.ElectionTimeOutMax,

		lastHeartbeatReceivedTime: time.Now(),

		logger:     params.Logger,
		members:    []common.ClusterMember{},
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
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
			if len(n.logs) == 0 {
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

func (n *RaftBrainImpl) Stop() {
	select {
	case n.stop <- struct{}{}:
	default:
	}
}

func (n *RaftBrainImpl) Start() {
	n.resetElectionTimeout()
	n.resetHeartBeatTimeout()

	go n.loop()

	n.log().Info().
		Interface("members", n.members).
		Msg("Brain start")
}

func (n *RaftBrainImpl) log() *zerolog.Logger {
	// data race
	sub := n.logger.With().
		Int("id", n.id).
		Str("state", n.state.String()).
		Int("votedFor", n.votedFor).
		Int("leaderId", n.votedFor).
		Int("term", n.currentTerm).
		Int("commitIndex", n.commitIndex).
		Int("lastApplied", n.lastApplied).
		Interface("nextIndex", n.nextIndex).
		Interface("matchIndex", n.matchIndex).
		Logger()
	return &sub
}
