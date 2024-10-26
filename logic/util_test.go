package logic

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/state_machine"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_DeleteFrom(t *testing.T) {
	ctx := context.TODO()

	sm, err := state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		DB: common.NewPersistenceMock(),
	})
	assert.NoError(t, err)

	sm.Reset()
	n := RaftBrainImpl{logs: []common.Log{}, db: common.NewPersistenceMock(), stateMachine: sm, commitIndex: 0, lastApplied: 0}
	n.applyLog(ctx)

	err = n.deleteLogFrom(ctx, 1)
	assert.ErrorIs(t, err, ErrLogIsEmpty)
	strLogs, err := n.db.ReadLogsToArray()
	assert.NoError(t, err)
	assert.Equal(t, strLogs, []string{})

	data := []common.Log{
		{Term: 1, Command: "set x 1"},
		{Term: 2, Command: "set x 2"},
		{Term: 3, Command: "set x 3"},
	}

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm, commitIndex: 3, lastApplied: 0}
	copy(n.logs, data)
	err = n.deleteLogFrom(ctx, 4)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)
	err = n.deleteLogFrom(ctx, 0)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)
	strLogs, err = n.db.ReadLogsToArray()
	assert.NoError(t, err)
	assert.Equal(t, strLogs, []string{})

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm, commitIndex: 3, lastApplied: 0}
	copy(n.logs, data)
	err = n.deleteLogFrom(ctx, 3)
	assert.NoError(t, err)
	assert.Equal(t, data[:2], n.logs)
	strLogs, err = n.db.ReadLogsToArray()
	assert.NoError(t, err)
	assert.Equal(t, strLogs, []string{"delete_log", "1"})

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm, commitIndex: 3, lastApplied: 0}
	copy(n.logs, data)
	err = n.deleteLogFrom(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, data[:1], n.logs)
	strLogs, err = n.db.ReadLogsToArray()
	assert.NoError(t, err)
	assert.Equal(t, strLogs, []string{"delete_log", "2"})

	sm.Reset()
	n = RaftBrainImpl{logs: make([]common.Log, 3), db: common.NewPersistenceMock(), stateMachine: sm, commitIndex: 3, lastApplied: 0}
	copy(n.logs, data)
	err = n.deleteLogFrom(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, []common.Log{}, n.logs)
	strLogs, err = n.db.ReadLogsToArray()
	assert.NoError(t, err)
	assert.Equal(t, strLogs, []string{"delete_log", "3"})
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

func TestRaftBrainImpl_deleteLogFrom(t *testing.T) {
	type fields struct {
		clusterClock              *ClusterClock
		dataFolder                string
		logger                    observability.Logger
		db                        Persistence
		members                   []common.ClusterMember
		nextMemberId              int
		state                     common.RaftState
		id                        int
		leaderID                  int
		stateMachine              SimpleStateMachine
		electionTimeOut           *time.Timer
		heartBeatTimeOut          *time.Timer
		heartBeatTimeOutMin       int64
		heartBeatTimeOutMax       int64
		electionTimeOutMin        int64
		electionTimeOutMax        int64
		rpcProxy                  RPCProxy
		arm                       AsyncResponseManager
		stop                      chan struct{}
		newMembers                chan common.ClusterMemberChange
		inOutLock                 sync.RWMutex
		changeMemberLock          sync.Mutex
		dataLock                  sync.RWMutex
		lastHeartbeatReceivedTime time.Time
		RpcRequestTimeout         time.Duration
		currentTerm               int
		votedFor                  int
		logs                      []common.Log
		commitIndex               int
		lastApplied               int
		nextIndex                 map[int]int
		matchIndex                map[int]int
	}
	type args struct {
		ctx   context.Context
		index int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				clusterClock:              tt.fields.clusterClock,
				dataFolder:                tt.fields.dataFolder,
				logger:                    tt.fields.logger,
				db:                        tt.fields.db,
				members:                   tt.fields.members,
				nextMemberId:              tt.fields.nextMemberId,
				state:                     tt.fields.state,
				id:                        tt.fields.id,
				leaderID:                  tt.fields.leaderID,
				stateMachine:              tt.fields.stateMachine,
				electionTimeOut:           tt.fields.electionTimeOut,
				heartBeatTimeOut:          tt.fields.heartBeatTimeOut,
				heartBeatTimeOutMin:       tt.fields.heartBeatTimeOutMin,
				heartBeatTimeOutMax:       tt.fields.heartBeatTimeOutMax,
				electionTimeOutMin:        tt.fields.electionTimeOutMin,
				electionTimeOutMax:        tt.fields.electionTimeOutMax,
				rpcProxy:                  tt.fields.rpcProxy,
				arm:                       tt.fields.arm,
				stop:                      tt.fields.stop,
				newMembers:                tt.fields.newMembers,
				inOutLock:                 tt.fields.inOutLock,
				changeMemberLock:          tt.fields.changeMemberLock,
				dataLock:                  tt.fields.dataLock,
				lastHeartbeatReceivedTime: tt.fields.lastHeartbeatReceivedTime,
				RpcRequestTimeout:         tt.fields.RpcRequestTimeout,
				currentTerm:               tt.fields.currentTerm,
				votedFor:                  tt.fields.votedFor,
				logs:                      tt.fields.logs,
				commitIndex:               tt.fields.commitIndex,
				lastApplied:               tt.fields.lastApplied,
				nextIndex:                 tt.fields.nextIndex,
				matchIndex:                tt.fields.matchIndex,
			}
			if err := n.deleteLogFrom(tt.args.ctx, tt.args.index); (err != nil) != tt.wantErr {
				t.Errorf("RaftBrainImpl.deleteLogFrom() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
