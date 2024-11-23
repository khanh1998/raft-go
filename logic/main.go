package logic

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

var (
	logger = otelslog.NewLogger("rpc-proxy")
)

type MembershipManager interface {
	Process(command any) error
	AddServer(input common.AddServerInput) error
}

// Persistent state on all servers:
// Updated on stable storage before responding to RPCs
// - currentTerm
// - votedFor
// - logs
type RaftPersistenceState interface {
	AppendLog(ctx context.Context, logItems []common.Log) (index int, err error)
	DeleteLogFrom(ctx context.Context, index int) (deletedLogs []common.Log, err error)
	DeleteAllLog(ctx context.Context) (err error)
	SetCurrentTerm(ctx context.Context, currentTerm int) (err error)
	SetVotedFor(ctx context.Context, votedFor int) (err error)
	StreamSnapshot(ctx context.Context, sm common.SnapshotMetadata, offset int64, maxLength int) (data []byte, eof bool, err error)
	InstallSnapshot(ctx context.Context, fileName string, offset int64, data []byte) (err error)
	CommitSnapshot(ctx context.Context, sm common.SnapshotMetadata) (err error)

	GetCurrentTerm() int            // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	GetVotedFor() int               // candidateId that received vote in current term (or null if none)
	LastLogInfo() (index, term int) // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	LogLength() int
	InMemoryLogLength() int // length of logs that exclude snapshot logs
	GetLog(index int) (common.Log, error)
	GetLastLog() (common.Log, error)
	GetLatestSnapshotMetadata() (snap common.SnapshotMetadata)
}

type MemberChangeSubscriber interface {
	// we send the latest cluster member config to subscriber,
	// they will figure out what are changed on they own
	InformMemberChange(ctx context.Context, members map[int]common.ClusterMember)
}

type NextOffset struct {
	Offset   int64
	FileName string // snapshot file name for receiver side, they will create new file with this name
	Snapshot common.SnapshotMetadata
}

// this struct only contains volatile data
type RaftBrainImpl struct {
	clusterClock              *ClusterClock
	logger                    observability.Logger
	members                   []common.ClusterMember
	memberChangeSubscribers   []MemberChangeSubscriber
	nextMemberId              int
	state                     common.RaftState
	id                        int
	leaderID                  int
	stateMachine              SimpleStateMachine
	electionTimeOut           *time.Timer
	heartBeatTimeOut          *time.Timer
	heartBeatTimeOutMin       time.Duration
	heartBeatTimeOutMax       time.Duration
	electionTimeOutMin        time.Duration
	electionTimeOutMax        time.Duration
	rpcProxy                  RPCProxy
	arm                       AsyncResponseManager
	stop                      context.CancelFunc // stop background goroutines
	newMembers                chan common.ClusterMemberChange
	inOutLock                 sync.RWMutex // this lock help to process requests in sequential order. requests are processed one after the other, not concurrently.
	changeMemberLock          sync.Mutex   // lock for adding or removing a member from the cluster
	dataLock                  sync.RWMutex // lock for reading or modifying internal data of the brain (consensus module)
	lastHeartbeatReceivedTime time.Time
	RpcRequestTimeout         time.Duration
	logLengthLimit            int
	snapshotChunkSize         int // in byte
	logFactory                common.LogFactory
	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs
	// currentTerm int          // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	// votedFor    int          // candidateId that received vote in current term (or null if none)
	// logs        []common.Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	persistState RaftPersistenceState

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// Reinitialized after election
	nextIndex  map[int]int        // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int        // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	nextOffset map[int]NextOffset // for peer that need to install snapshot
}

func (k *RaftBrainImpl) NotifyMemberChange(ctx context.Context) {
	members := map[int]common.ClusterMember{}
	for _, subscriber := range k.memberChangeSubscribers {
		subscriber.InformMemberChange(ctx, members)
	}
}

func (k *RaftBrainImpl) SubscribeToMemberChangeEvent(subscriber MemberChangeSubscriber) {
	k.memberChangeSubscribers = append(k.memberChangeSubscribers, subscriber)
	// immediately send current cluster member config to new subscriber
	go subscriber.InformMemberChange(context.Background(), nil)
}

type AsyncResponseItem struct {
	Response common.LogResult
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
	Response    string
	Err         error
}

type SimpleStateMachine interface {
	Reset(ctx context.Context) error
	Process(ctx context.Context, logIndex int, log common.Log) (result common.LogResult, err error)
	StartSnapshot(ctx context.Context) error
	GetLastConfig() map[int]common.ClusterMember
}

type RPCProxy interface {
	SendAppendEntries(ctx context.Context, peerId int, timeout *time.Duration, input common.AppendEntriesInput) (output common.AppendEntriesOutput, err error)
	SendRequestVote(ctx context.Context, peerId int, timeout *time.Duration, input common.RequestVoteInput) (output common.RequestVoteOutput, err error)
	SendInstallSnapshot(ctx context.Context, peerId int, timeout *time.Duration, input common.InstallSnapshotInput) (output common.InstallSnapshotOutput, err error)
	SendPing(ctx context.Context, peerId int, timeout *time.Duration) (res common.PingResponse, err error)

	ConnectToNewPeer(ctx context.Context, peerID int, peerURL string, retry int, retryDelay time.Duration) error
	SendToVotingMember(ctx context.Context, peerId int, timeout *time.Duration) (err error)
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
	HeartBeatTimeOutMin time.Duration
	HeartBeatTimeOutMax time.Duration
	ElectionTimeOutMin  time.Duration
	ElectionTimeOutMax  time.Duration
	Logger              observability.Logger
	CachingUp           bool // will be ignored if the cluster mode is STATIC
	RpcRequestTimeout   time.Duration
	PersistenceState    RaftPersistenceState // this will take care of raft's states that need to be persisted
	LogLengthLimit      int
	SnapshotChunkSize   int
	LogFactory          common.LogFactory
}

func NewRaftBrain(params NewRaftBrainParams) (*RaftBrainImpl, error) {
	latestSnapshot := params.PersistenceState.GetLatestSnapshotMetadata()
	clusterClock := NewClusterClock()

	n := &RaftBrainImpl{
		id: params.ID,
		state: func() common.RaftState {
			if params.CachingUp {
				return common.StateCatchingUp
			}
			return common.StateFollower
		}(),
		arm:          NewAsyncResponseManager(100),
		stop:         nil,
		newMembers:   make(chan common.ClusterMemberChange, 25),
		nextMemberId: params.ID + 1,

		heartBeatTimeOutMin: params.HeartBeatTimeOutMin,
		heartBeatTimeOutMax: params.HeartBeatTimeOutMax,
		electionTimeOutMin:  params.ElectionTimeOutMin,
		electionTimeOutMax:  params.ElectionTimeOutMax,

		lastHeartbeatReceivedTime: time.Now(),

		logger:            params.Logger,
		members:           []common.ClusterMember{},
		nextIndex:         make(map[int]int),
		matchIndex:        make(map[int]int),
		nextOffset:        make(map[int]NextOffset),
		clusterClock:      clusterClock,
		RpcRequestTimeout: params.RpcRequestTimeout,

		persistState:      params.PersistenceState,
		logLengthLimit:    params.LogLengthLimit,
		snapshotChunkSize: params.SnapshotChunkSize,
		logFactory:        params.LogFactory,

		// only committed logs are compacted into snapshot,
		// for new cluster, it will be zero.
		commitIndex: latestSnapshot.LastLogIndex,
		lastApplied: latestSnapshot.LastLogIndex,
	}

	ctx, span := tracer.Start(context.Background(), "NewRaftBrain")
	defer span.End()

	if params.Mode == common.Dynamic {
		if params.CachingUp {
			// if the new node is in catching up mode,
			// we ignore all member configurations, because it hasn't been a part of cluster yet.

			// todo: fix this,
			// still get member configurations from local file
			if len(params.Members) > 0 {
				return nil, errors.New("in catching up mode, we don't pass member list as parameter")
			}
		} else {
			if len(params.Members) != 1 {
				return nil, errors.New("in dynamic cluster, initially there is one server")
			}

			// if this is the first node of cluster and the first time the node get boosted up,
			// initially add members to the cluster. otherwise, it's already in the log.
			if n.persistState.LogLength() == 0 {
				mem := params.Members[0]
				n.appendLog(ctx, n.logFactory.AddNewNode(1, 0, mem.ID, mem.HttpUrl, mem.RpcUrl))
			} else {
				for _, mem := range params.Members {
					err := n.addMember(mem.ID, mem.HttpUrl, mem.RpcUrl)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	if params.Mode == common.Static {
		if len(params.Members) == 0 {
			return nil, errors.New("in static cluster, you need to pass the member list of cluster")
		}

		for _, mem := range params.Members {
			err := n.addMember(mem.ID, mem.HttpUrl, mem.RpcUrl)
			if err != nil {
				return nil, err
			}
		}
	}

	return n, nil
}

func (n *RaftBrainImpl) Stop() {
	n.stop()
}

func (n *RaftBrainImpl) Start(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Start")
	defer span.End()

	n.resetElectionTimeout(ctx)
	n.resetHeartBeatTimeout(ctx)

	ctx, cancel := context.WithCancel(ctx)
	n.stop = cancel

	go n.loop(ctx)
	// periodically inject committed logs to state machine
	go n.logInjector(ctx)

	go n.autoCommitClusterTime(ctx)

	n.log().InfoContext(ctx, "Brain start", "members", n.members)
}

// data race
func (n *RaftBrainImpl) log() observability.Logger {
	sub := n.logger.With(
		"id", n.id,
		"state", n.state.String(),
		"votedFor", n.GetVotedFor(),
		"leaderID", n.leaderID,
		"currentTerm", n.GetCurrentTerm(),
		"commitIndex", n.commitIndex,
		"lastApplied", n.lastApplied,
		"nextIndex", n.nextIndex,
		"matchIndex", n.matchIndex,
		"nextOffset", n.nextOffset,
	)

	return sub
}
