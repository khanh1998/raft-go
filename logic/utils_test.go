package logic

import (
	"khanh/raft-go/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_isLogUpToDate(t *testing.T) {
	type TestCase struct {
		name         string
		lastLogIndex int
		lastLogTerm  int
		n            NodeImpl
		output       bool
	}

	testCases := []TestCase{
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            NodeImpl{Logs: []common.Log{}},
			output:       true,
		},
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            NodeImpl{Logs: []common.Log{{Term: 1}, {Term: 2}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex = index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            NodeImpl{Logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex > index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n:            NodeImpl{Logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex < index",
			lastLogIndex: 1,
			lastLogTerm:  5,
			n:            NodeImpl{Logs: []common.Log{{Term: 1}, {Term: 5}}},
			output:       false,
		},
		{
			name:         "lastLogTerm < term",
			lastLogIndex: 3,
			lastLogTerm:  3,
			n:            NodeImpl{Logs: []common.Log{{Term: 3}, {Term: 4}}},
			output:       false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.output, testCase.n.isLogUpToDate(testCase.lastLogIndex, testCase.lastLogTerm), testCase.name)
	}
}
