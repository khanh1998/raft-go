package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/state_machine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_DeleteFrom(t *testing.T) {
	sm, err := state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		DB: common.NewPersistenceMock(),
	})
	assert.NoError(t, err)

	sm.Reset()
	n := RaftBrainImpl{logs: []common.Log{}, db: common.NewPersistenceMock(), stateMachine: sm}
	err = n.deleteLogFrom(1)
	assert.ErrorIs(t, err, ErrLogIsEmtpy)

	data := []common.Log{
		{Term: 1, Command: "set x 1"},
		{Term: 2, Command: "set x 2"},
		{Term: 3, Command: "set x 3"},
	}

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm}
	copy(n.logs, data)
	err = n.deleteLogFrom(4)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)
	err = n.deleteLogFrom(0)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm}
	copy(n.logs, data)
	err = n.deleteLogFrom(3)
	assert.NoError(t, err)
	assert.Equal(t, data[:2], n.logs)

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm}
	copy(n.logs, data)
	err = n.deleteLogFrom(2)
	assert.NoError(t, err)
	assert.Equal(t, data[:1], n.logs)

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm}
	copy(n.logs, data)
	err = n.deleteLogFrom(1)
	assert.NoError(t, err)
	assert.Equal(t, []common.Log{}, n.logs)
}

func Test_nodeImpl_isLogUpToDate(t *testing.T) {
	type TestCase struct {
		name         string
		lastLogIndex int
		lastLogTerm  int
		n            RaftBrainImpl
		output       bool
	}

	testCases := []TestCase{
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{logs: []common.Log{}},
			output:       true,
		},
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{logs: []common.Log{{Term: 1}, {Term: 2}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex = index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex > index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex < index",
			lastLogIndex: 1,
			lastLogTerm:  5,
			n:            RaftBrainImpl{logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       false,
		},
		{
			name:         "lastLogTerm < term",
			lastLogIndex: 3,
			lastLogTerm:  3,
			n:            RaftBrainImpl{logs: []common.Log{{Term: 3}, {Term: 4}}},
			output:       false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.output, testCase.n.isLogUpToDate(testCase.lastLogIndex, testCase.lastLogTerm), testCase.name)
	}
}
