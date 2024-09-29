package logic

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/state_machine"
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

	logger := observability.NewZerolog("", 1)

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n: RaftBrainImpl{
				currentTerm: 3, logger: logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 2},
			out: common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgRequesterTermIsOutDated},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				currentTerm: 3, votedFor: 4, db: common.NewPersistenceMock(), logger: logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 3},
			out: common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				currentTerm: 3, votedFor: 0, logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 3}},
				db: common.NewPersistenceMock(), logger: logger,
				electionTimeOutMin: 300,
				electionTimeOutMax: 500,
			},
			in:  common.RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 2},
			out: common.RequestVoteOutput{Term: 4, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				currentTerm:         3,
				votedFor:            0,
				logs:                []common.Log{{Term: 1}, {Term: 2}, {Term: 3}},
				db:                  common.NewPersistenceMock(),
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
		var out common.RequestVoteOutput
		testCase.n.RequestVote(context.TODO(), &testCase.in, &out)
		log.Info().Int("index", index).Msg("test case RequestVoteOutput")
		assert.Equal(t, testCase.out, out, fmt.Sprintf("test case: #%d", index))
	}
}
func Test_nodeImpl_AppendEntries(t *testing.T) {
	type TestCasePersist struct {
		do       bool
		logCount int
	}

	type TestCase struct {
		name    string
		n       RaftBrainImpl
		in      common.AppendEntriesInput
		out     common.AppendEntriesOutput
		persist TestCasePersist
	}

	logger := observability.NewZerolog("", 1)

	sm, err := state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{DB: common.NewPersistenceMock()})
	assert.NoError(t, err)

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n: RaftBrainImpl{
				currentTerm:         5,
				logger:              logger,
				db:                  common.NewPersistenceMock(),
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				currentTerm:         2,
				logs:                []common.Log{{Term: 1}, {Term: 1}},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				currentTerm:         2,
				logs:                []common.Log{},
				logger:              logger,
				db:                  common.NewPersistenceMock(),
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				currentTerm:         2,
				logs:                []common.Log{{Term: 1}},
				logger:              logger,
				db:                  common.NewPersistenceMock(),
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				votedFor:    5,
				currentTerm: 3,
				logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set x 5"},
					{Term: 2, Command: "set x 5"},
				},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				votedFor:            5,
				currentTerm:         3,
				logs:                []common.Log{},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				votedFor:    5,
				currentTerm: 3,
				logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set y 5"},
				},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				votedFor:    5,
				currentTerm: 3,
				logs: []common.Log{
					{Term: 1, Command: "set x 5"},
					{Term: 2, Command: "set y 5"},
				},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
			n: RaftBrainImpl{
				votedFor:            1,
				currentTerm:         1,
				logs:                []common.Log{},
				db:                  common.NewPersistenceMock(),
				logger:              logger,
				heartBeatTimeOutMin: 100,
				heartBeatTimeOutMax: 150,
				electionTimeOutMin:  300,
				electionTimeOutMax:  500,
				stateMachine:        sm,
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
		testCase.n.stateMachine.Reset()
		t.Run(fmt.Sprintf("[%d] %s", index, testCase.name), func(t *testing.T) {
			log.Info().Int("index", index).Msg("test case AppendEntriesOutput")
			var out common.AppendEntriesOutput
			testCase.n.AppendEntries(context.TODO(), &testCase.in, &out)
			assert.Equal(t, testCase.out, out, fmt.Sprintf("%d test case: %s", index, testCase.name))

			if testCase.persist.do {
				n2 := RaftBrainImpl{
					db:                  testCase.n.db,
					logger:              logger,
					heartBeatTimeOutMin: 100,
					heartBeatTimeOutMax: 150,
					electionTimeOutMin:  300,
					electionTimeOutMax:  500,
					stateMachine:        testCase.n.stateMachine,
				}
				keys, err := n2.getPersistanceKeyList()
				assert.NoError(t, err)
				// assert.Equal(t, []string{}, keys)
				data, err := n2.db.ReadNewestLog(keys)
				assert.NoError(t, err)
				// assert.Equal(t, map[string]string{}, data)
				_ = data

				err = n2.restoreRaftStateFromFile(context.TODO())

				assert.NoError(t, err)

				assert.Equal(t, testCase.n.currentTerm, n2.currentTerm)
				assert.Equal(t, testCase.n.votedFor, n2.votedFor)
				assert.Equal(t, testCase.n.logs, n2.logs)
				assert.Equal(t, testCase.persist.logCount, len(n2.logs))
			}
		})
	}
}
