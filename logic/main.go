package logic

import (
	"fmt"
	"khanh/raft-go/persistance"
	"math"
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ClientRequest struct {
	Data []Entry `json:"data"`
}

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
	Peers             []*rpc.Client
	State             ServerState
	ID                int
	StateMachine      StateMachine
	ElectionTimeOut   *time.Timer
	HeartBeatTimeOut  *time.Timer
	ClientRequests    chan ClientRequest
	stop              chan struct{}
	Quorum            int
	MinRandomDuration int64
	MaxRandomDuration int64
	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs
	CurrentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int   // candidateId that received vote in current term (or null if none)
	Logs        []Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	CommitedIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	LastApplied   int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// Reinitialized after election
	NextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type Node interface {
	RequestVote(input *RequestVoteInput, output *RequestVoteOutput) (err error)
	AppendEntries(input *AppendEntriesInput, output *AppendEntriesOutput) (err error)
	Stop() chan struct{}
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

func NewNode(params NewNodeParams) (Node, error) {
	n := &NodeImpl{
		ID:             params.ID,
		State:          StateFollower,
		VotedFor:       0,
		Quorum:         int(math.Ceil(float64(len(params.PeerRpcURLs)) / 2.0)),
		DB:             persistance.NewPersistence(params.DataFileName),
		ClientRequests: make(chan ClientRequest),
		stop:           make(chan struct{}),

		MinRandomDuration: params.MinRandomDuration,
		MaxRandomDuration: params.MaxRandomDuration,
		logger:            params.Log,
	}

	n.initRPC(params.RpcHostURL)
	n.initApi(params.RestApiHostURL)

	err := n.Rehydrate()
	if err != nil {
		return n, err
	}

	for i := 0; i < len(params.PeerRpcURLs); i++ {
		n.NextIndex = append(n.NextIndex, len(n.Logs)+1)
		n.MatchIndex = append(n.MatchIndex, 0)
	}

	n.ConnectToPeers(params.PeerRpcURLs)

	go n.loop()
	return n, nil
}

func (n *NodeImpl) log() *zerolog.Logger {
	sub := n.logger.With().
		Str("state", n.State.String()).
		Int("voted for", n.VotedFor).
		Int("term", n.CurrentTerm).
		Logger()
	return &sub
}

func (n *NodeImpl) ConnectToPeers(peerRpcURLs []string) {
	var count sync.WaitGroup
	for _, url := range peerRpcURLs {
		count.Add(1)
		go func(url string) {
			for i := 0; i < 5; i++ {
				n.log().Info().Msgf("dialing %s", url)
				client, err := rpc.Dial("tcp", url)
				if err != nil {
					time.Sleep(3 * time.Second)
					n.log().Err(err).Msg("Client connection error: ")
					continue
				} else {
					n.log().Info().Msgf("connect to %s successfully", url)
					n.Peers = append(n.Peers, client)

					var message string
					err = client.Call("NodeImpl.Ping", fmt.Sprintf("Node %v", n.ID), &message)
					if err != nil {
						log.Err(err).Str("url", url).Msg("cannot ping")
					} else {
						log.Info().Msg(message)
					}

					break
				}

			}
			count.Done()
		}(url)
	}

	count.Wait()

	if len(n.Peers) == len(peerRpcURLs) {
		n.log().Info().Msg("Successfully connected to all peers")
	} else {
		n.log().Fatal().Msg("cannot connect to all peers")
	}

	n.resetElectionTimeout()
	n.resetHeartBeatTimeout()
}

func (n NodeImpl) Stop() chan struct{} {
	return n.stop
}

func (n *NodeImpl) resetElectionTimeout() {
	randomElectionTimeOut := time.Duration(RandInt(n.MaxRandomDuration, n.MaxRandomDuration+n.MinRandomDuration)) * time.Millisecond
	n.log().Info().Interface("seconds", randomElectionTimeOut.Seconds()).Msg("resetElectionTimeout")
	if n.ElectionTimeOut == nil {
		n.ElectionTimeOut = time.NewTimer(randomElectionTimeOut)
	} else {
		n.ElectionTimeOut.Reset(randomElectionTimeOut)
	}
}

func (n *NodeImpl) resetHeartBeatTimeout() {
	randomHeartBeatTimeout := time.Duration(RandInt(n.MinRandomDuration, n.MaxRandomDuration)) * time.Millisecond
	n.log().Info().Interface("seconds", randomHeartBeatTimeout.Seconds()).Msg("resetHeartBeatTimeout")
	if n.HeartBeatTimeOut == nil {
		n.HeartBeatTimeOut = time.NewTimer(randomHeartBeatTimeout)
	} else {
		n.HeartBeatTimeOut.Reset(randomHeartBeatTimeout)
	}
}

type AppendEntriesInput struct {
	Term         int     // leader’s term
	LeaderID     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesOutput struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Message string
}

type RequestVoteInput struct {
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

type RequestVoteOutput struct {
	Term        int    // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
	Message     string // for debuging purpose
}
