package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_DeleteFrom(t *testing.T) {
	n := RaftBrainImpl{Logs: []common.Log{}, DB: persistance.NewPersistenceMock()}
	err := n.DeleteLogFrom(1)
	assert.ErrorIs(t, err, ErrLogIsEmtpy)

	data := []common.Log{
		{Term: 1, Command: "set x 1"},
		{Term: 2, Command: "set x 2"},
		{Term: 3, Command: "set x 3"},
	}

	n = RaftBrainImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(4)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)
	err = n.DeleteLogFrom(0)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)

	n = RaftBrainImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(3)
	assert.NoError(t, err)
	assert.Equal(t, data[:2], n.Logs)

	n = RaftBrainImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(2)
	assert.NoError(t, err)
	assert.Equal(t, data[:1], n.Logs)

	n = RaftBrainImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(1)
	assert.NoError(t, err)
	assert.Equal(t, []common.Log{}, n.Logs)
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
			n:            RaftBrainImpl{Logs: []common.Log{}},
			output:       true,
		},
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{Logs: []common.Log{{Term: 1}, {Term: 2}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex = index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{Logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex > index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            RaftBrainImpl{Logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex < index",
			lastLogIndex: 1,
			lastLogTerm:  5,
			n:            RaftBrainImpl{Logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       false,
		},
		{
			name:         "lastLogTerm < term",
			lastLogIndex: 3,
			lastLogTerm:  3,
			n:            RaftBrainImpl{Logs: []common.Log{{Term: 3}, {Term: 4}}},
			output:       false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.output, testCase.n.isLogUpToDate(testCase.lastLogIndex, testCase.lastLogTerm), testCase.name)
	}
}
