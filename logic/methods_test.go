package logic

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
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

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n:    RaftBrainImpl{CurrentTerm: 3, logger: &log.Logger},
			in:   common.RequestVoteInput{Term: 2},
			out:  common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgRequesterTermIsOutDated},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n:    RaftBrainImpl{CurrentTerm: 3, VotedFor: 4, DB: persistance.NewPersistenceMock(), logger: &log.Logger},
			in:   common.RequestVoteInput{Term: 4},
			out:  common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n:    RaftBrainImpl{CurrentTerm: 3, VotedFor: 0, Logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 3}}, DB: persistance.NewPersistenceMock(), logger: &log.Logger},
			in:   common.RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 2},
			out:  common.RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: RaftBrainImpl{
				CurrentTerm:       3,
				VotedFor:          0,
				Logs:              []common.Log{{Term: 1}, {Term: 2}, {Term: 3}},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MaxRandomDuration: 10000,
				MinRandomDuration: 5000,
			},
			in:  common.RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 3},
			out: common.RequestVoteOutput{Term: 3, VoteGranted: true, Message: ""},
		},
	}

	for index, testCase := range testCases {
		var out common.RequestVoteOutput
		testCase.n.RequestVote(&testCase.in, &out)
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

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n: RaftBrainImpl{
				CurrentTerm:       5,
				logger:            &log.Logger,
				DB:                persistance.NewPersistenceMock(),
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
				CurrentTerm:       2,
				Logs:              []common.Log{{Term: 1}, {Term: 1}},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
				CurrentTerm:       2,
				Logs:              []common.Log{},
				logger:            &log.Logger,
				DB:                persistance.NewPersistenceMock(),
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
				CurrentTerm:       2,
				Logs:              []common.Log{{Term: 1}},
				logger:            &log.Logger,
				DB:                persistance.NewPersistenceMock(),
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Values: []common.Entry{
						{Key: "x", Value: 5, Opcode: common.Overwrite}},
					},
					{Term: 2, Values: []common.Entry{
						{Key: "x", Value: 5, Opcode: common.Overwrite}},
					},
					{Term: 2, Values: []common.Entry{
						{Key: "x", Value: 5, Opcode: common.Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
				VotedFor:          5,
				CurrentTerm:       3,
				Logs:              []common.Log{},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []common.Log{
					{
						Term: 1, //
						Values: []common.Entry{
							{Key: "z", Value: 3, Opcode: common.Overwrite},
						},
					},
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
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Values: []common.Entry{
						{Key: "x", Value: 5, Opcode: common.Overwrite}},
					},
					{Term: 2, Values: []common.Entry{
						{Key: "y", Value: 5, Opcode: common.Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: common.AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
				Entries: []common.Log{
					{
						Term: 1,
						Values: []common.Entry{
							{Key: "z", Value: 3, Opcode: common.Overwrite},
						},
					},
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
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Values: []common.Entry{
						{Key: "x", Value: 5, Opcode: common.Overwrite}},
					},
					{Term: 2, Values: []common.Entry{
						{Key: "y", Value: 5, Opcode: common.Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
				StateMachine:      common.NewStateMachine(),
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
				VotedFor:          1,
				CurrentTerm:       1,
				Logs:              []common.Log{},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
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
		log.Info().Int("index", index).Msg("test case AppendEntriesOutput")
		var out common.AppendEntriesOutput
		testCase.n.AppendEntries(&testCase.in, &out)
		assert.Equal(t, testCase.out, out, fmt.Sprintf("%d test case: %s", index, testCase.name))

		if testCase.persist.do {
			n2 := RaftBrainImpl{
				DB:                testCase.n.DB,
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			}
			keys, err := n2.GetPersistanceKeyList()
			assert.NoError(t, err)
			// assert.Equal(t, []string{}, keys)
			data, err := n2.DB.ReadNewestLog(keys)
			assert.NoError(t, err)
			// assert.Equal(t, map[string]string{}, data)
			_ = data

			err = n2.Rehydrate()

			assert.NoError(t, err)

			assert.Equal(t, testCase.n.CurrentTerm, n2.CurrentTerm)
			assert.Equal(t, testCase.n.VotedFor, n2.VotedFor)
			assert.Equal(t, testCase.n.Logs, n2.Logs)
			assert.Equal(t, testCase.persist.logCount, len(n2.Logs))
		}
	}
}
