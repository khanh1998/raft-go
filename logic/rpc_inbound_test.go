package logic

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/persistence_state"
	"khanh/raft-go/state_machine"
	"khanh/raft-go/storage"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_RequestVote(t *testing.T) {
	type TestCase struct {
		name string
		n    RaftBrainImpl
		in   common.RequestVoteInput
		out  common.RequestVoteOutput
	}

	logger := observability.NewZerolog(common.ObservabilityConfig{}, 1)

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n: RaftBrainImpl{
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					CurrentTerm: 3,
					Storage: storage.NewStorageForTest(
						storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
						storage.NewFileWrapperMock(),
					),
				}),
				logger:             logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 2},
			out: common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgRequesterTermIsOutDated},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					CurrentTerm: 3,
					VotedFor:    4,
					Storage: storage.NewStorageForTest(
						storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
						storage.NewFileWrapperMock(),
					),
				}),
				logger:             logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 3},
			out: common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					CurrentTerm: 3,
					VotedFor:    0,
					Logs:        []common.Log{{Term: 1}, {Term: 2}, {Term: 3}},
					Storage: storage.NewStorageForTest(
						storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
						storage.NewFileWrapperMock(),
					),
				}),
				logger:             logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 2},
			out: common.RequestVoteOutput{Term: 4, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					CurrentTerm: 3,
					VotedFor:    0,
					Logs:        []common.Log{{Term: 1}, {Term: 2}, {Term: 3}},
					Storage: storage.NewStorageForTest(
						storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
						storage.NewFileWrapperMock(),
					),
				}),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
			},
			in:  common.RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 3},
			out: common.RequestVoteOutput{Term: 4, VoteGranted: true, Message: ""},
		},
	}

	for index, testCase := range testCases {
		if index == 2 {
			logger.Info("breakpoint")
		}
		logger.Info(testCase.name)
		var out common.RequestVoteOutput
		testCase.n.RequestVote(context.TODO(), &testCase.in, &out)
		log.Info().Int("index", index).Msg("test case RequestVoteOutput")
		assert.Equal(t, testCase.out, out, fmt.Sprintf("test case: #%d", index))
		logger.Info(testCase.name)
	}
}
func Test_nodeImpl_AppendEntries(t *testing.T) {
	type TestCasePersist struct {
		do       bool
		logCount int
	}

	type TestCase struct {
		name    string
		ps      *persistence_state.RaftPersistenceStateImpl
		n       RaftBrainImpl
		in      common.AppendEntriesInput
		out     common.AppendEntriesOutput
		persist TestCasePersist
	}

	logger := observability.NewZerolog(common.ObservabilityConfig{}, 1)

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				CurrentTerm: 5,
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
				persistState:        nil,
			},
			in: common.AppendEntriesInput{
				Term: 4,
			},
			out: common.AppendEntriesOutput{
				Success: false,
				Term:    5,
				Message: MsgRequesterTermIsOutDated,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				CurrentTerm: 2,
				Logs:        []common.Log{{Term: 1}, {Term: 1}},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: common.AppendEntriesOutput{
				Success: false,
				Term:    3,
				Message: MsgPreviousLogTermsAreNotMatched,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				CurrentTerm: 2,
				Logs:        []common.Log{},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: common.AppendEntriesOutput{
				Success: false,
				Term:    3,
				Message: MsgTheResponderHasNoLog,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				CurrentTerm: 2,
				Logs:        []common.Log{{Term: 1}},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: common.AppendEntriesOutput{
				Success: false,
				Term:    3,
				Message: MsgTheResponderHasFewerLogThanRequester,
			},
		},
		{
			name: "3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set x 5"},
					{Term: 2, Command: "set x 5"},
				},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: common.AppendEntriesOutput{
				Success: false,
				Term:    3,
				Message: MsgCurrentLogTermsAreNotMatched,
			},
			persist: TestCasePersist{do: true, logCount: 2},
		},
		{
			name: "4. Append any new entries not already in the log",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs:        []common.Log{},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []common.Log{
					{Term: 1, Command: "set z 3"},
				},
			},
			out: common.AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
			persist: TestCasePersist{do: true, logCount: 1},
		},
		{
			name: "4. Append any new entries not already in the log",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set y 5"},
				},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
				Entries: []common.Log{
					{Term: 1, Command: "set z 3"},
				},
			},
			out: common.AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
			persist: TestCasePersist{do: true, logCount: 3},
		},
		{
			name: "5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set y 5"},
				},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
				LeaderCommit: 6,
			},
			out: common.AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
		},
		{
			name: "5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)",
			ps: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
				VotedFor:    1,
				CurrentTerm: 1,
				Logs:        []common.Log{},
				Storage: storage.NewStorageForTest(
					storage.NewStorageParams{WalSize: 1024, DataFolder: "data/", Logger: logger},
					storage.NewFileWrapperMock(),
				),
			}),
			n: RaftBrainImpl{
				persistState:        nil,
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        nil,
				clusterClock:        NewClusterClock(),
			},
			in: common.AppendEntriesInput{
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				LeaderCommit: 1,
			},
			out: common.AppendEntriesOutput{
				Success: true,
				Term:    1,
				Message: "",
			},
		},
	}

	for index, testCase := range testCases {
		testCase := &testCase
		t.Run(fmt.Sprintf("[%d] %s", index, testCase.name), func(t *testing.T) {
			if index == 2 {
				log.Info().Msg("breakpoint")
			}
			testCase.n.persistState = testCase.ps
			testCase.n.stateMachine = state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
				PersistState: testCase.ps,
			})
			log.Info().Int("index", index).Msg("test case AppendEntriesOutput")
			var out common.AppendEntriesOutput
			testCase.n.AppendEntries(context.TODO(), &testCase.in, &out)
			assert.Equal(t, testCase.out, out, fmt.Sprintf("%d test case: %s", index, testCase.name))
		})
	}
}
