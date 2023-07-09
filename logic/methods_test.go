package logic

import (
	"fmt"
	"khanh/raft-go/persistance"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_RequestVote(t *testing.T) {
	type TestCase struct {
		name string
		n    NodeImpl
		in   RequestVoteInput
		out  RequestVoteOutput
	}

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n:    NodeImpl{CurrentTerm: 3, logger: &log.Logger},
			in:   RequestVoteInput{Term: 2},
			out:  RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgRequesterTermIsOutDated},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n:    NodeImpl{CurrentTerm: 3, VotedFor: 4, DB: persistance.NewPersistenceMock(), logger: &log.Logger},
			in:   RequestVoteInput{Term: 4},
			out:  RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n:    NodeImpl{CurrentTerm: 3, VotedFor: 0, Logs: []Log{{Term: 1}, {Term: 2}, {Term: 3}}, DB: persistance.NewPersistenceMock(), logger: &log.Logger},
			in:   RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 2},
			out:  RequestVoteOutput{Term: 3, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate},
		},
		{
			name: "2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)",
			n: NodeImpl{
				CurrentTerm:       3,
				VotedFor:          0,
				Logs:              []Log{{Term: 1}, {Term: 2}, {Term: 3}},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MaxRandomDuration: 10000,
				MinRandomDuration: 5000,
			},
			in:  RequestVoteInput{Term: 4, LastLogIndex: 4, LastLogTerm: 3},
			out: RequestVoteOutput{Term: 3, VoteGranted: true, Message: ""},
		},
	}

	for index, testCase := range testCases {
		var out RequestVoteOutput
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
		n       NodeImpl
		in      AppendEntriesInput
		out     AppendEntriesOutput
		persist TestCasePersist
	}

	testCases := []TestCase{
		{
			name: "1. Reply false if term < currentTerm (§5.1)",
			n: NodeImpl{
				CurrentTerm:       5,
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term: 4,
			},
			out: AppendEntriesOutput{
				Success: false,
				Term:    5,
				Message: MsgRequesterTermIsOutDated,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			n: NodeImpl{
				CurrentTerm:       2,
				Logs:              []Log{{Term: 1}, {Term: 1}},
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: AppendEntriesOutput{
				Success: false,
				Term:    2,
				Message: MsgPreviousLogTermsAreNotMatched,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			n: NodeImpl{
				CurrentTerm:       2,
				Logs:              []Log{},
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: AppendEntriesOutput{
				Success: false,
				Term:    2,
				Message: MsgTheResponderHasNoLog,
			},
		},
		{
			name: "2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)",
			n: NodeImpl{
				CurrentTerm:       2,
				Logs:              []Log{{Term: 1}},
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: AppendEntriesOutput{
				Success: false,
				Term:    2,
				Message: MsgTheResponderHasFewerLogThanRequester,
			},
		},
		{
			name: "3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)",
			n: NodeImpl{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []Log{
					{Term: 1, Values: []Entry{
						{Key: "x", Value: 5, Opcode: Overwrite}},
					},
					{Term: 2, Values: []Entry{
						{Key: "x", Value: 5, Opcode: Overwrite}},
					},
					{Term: 2, Values: []Entry{
						{Key: "x", Value: 5, Opcode: Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
			},
			out: AppendEntriesOutput{
				Success: false,
				Term:    3,
				Message: MsgCurrentLogTermsAreNotMatched,
			},
			persist: TestCasePersist{do: true, logCount: 2},
		},
		{
			name: "4. Append any new entries not already in the log",
			n: NodeImpl{
				VotedFor:          5,
				CurrentTerm:       3,
				Logs:              []Log{},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []Entry{
					{Key: "z", Value: 3, Opcode: Overwrite},
				},
			},
			out: AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
			persist: TestCasePersist{do: true, logCount: 1},
		},
		{
			name: "4. Append any new entries not already in the log",
			n: NodeImpl{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []Log{
					{Term: 1, Values: []Entry{
						{Key: "x", Value: 5, Opcode: Overwrite}},
					},
					{Term: 2, Values: []Entry{
						{Key: "y", Value: 5, Opcode: Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
				Entries: []Entry{
					{Key: "z", Value: 3, Opcode: Overwrite},
				},
			},
			out: AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
			persist: TestCasePersist{do: true, logCount: 3},
		},
		{
			name: "5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)",
			n: NodeImpl{
				VotedFor:    5,
				CurrentTerm: 3,
				Logs: []Log{
					{Term: 1, Values: []Entry{
						{Key: "x", Value: 5, Opcode: Overwrite}},
					},
					{Term: 2, Values: []Entry{
						{Key: "y", Value: 5, Opcode: Overwrite}},
					},
				},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         3,
				PrevLogIndex: 2,
				PrevLogTerm:  2,
				LeaderCommit: 6,
			},
			out: AppendEntriesOutput{
				Success: true,
				Term:    3,
				Message: "",
			},
		},
		{
			name: "5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)",
			n: NodeImpl{
				VotedFor:          1,
				CurrentTerm:       1,
				Logs:              []Log{},
				DB:                persistance.NewPersistenceMock(),
				logger:            &log.Logger,
				MinRandomDuration: 1000,
				MaxRandomDuration: 10000,
			},
			in: AppendEntriesInput{
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				LeaderCommit: 1,
			},
			out: AppendEntriesOutput{
				Success: true,
				Term:    1,
				Message: "",
			},
		},
	}

	for index, testCase := range testCases {
		log.Info().Int("index", index).Msg("test case AppendEntriesOutput")
		var out AppendEntriesOutput
		testCase.n.AppendEntries(&testCase.in, &out)
		assert.Equal(t, testCase.out, out, fmt.Sprintf("%d test case: %s", index, testCase.name))

		if testCase.persist.do {
			n2 := NodeImpl{
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
