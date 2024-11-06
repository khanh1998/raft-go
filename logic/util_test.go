package logic

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/state_machine"
	"khanh/raft-go/storage"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_DeleteFrom(t *testing.T) {
	ctx := context.TODO()

	logger := observability.NewSimpleLog()

	ps := common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
		Logs: []common.Log{},
		Storage: storage.NewStorageForTest(storage.NewStorageParams{
			WalSize:    1000,
			DataFolder: "data/",
			Logger:     logger,
		}, storage.NewFileWrapperMock()),
	})
	sm := state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		PersistState: ps,
	})
	n := RaftBrainImpl{
		persistState: ps,
		stateMachine: sm, commitIndex: 0, lastApplied: 0}
	n.applyLog(ctx)

	err := n.deleteLogFrom(ctx, 1)
	assert.ErrorIs(t, err, common.ErrLogIsEmpty)

	data := []common.Log{
		{Term: 1, Command: "set x 1"},
		{Term: 2, Command: "set x 2"},
		{Term: 3, Command: "set x 3"},
	}

	copyData := func() []common.Log {
		d := make([]common.Log, len(data))
		copy(d, data)
		return d
	}

	ps = common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
		Logs: copyData(),
		Storage: storage.NewStorageForTest(storage.NewStorageParams{
			WalSize:    1000,
			DataFolder: "data/",
			Logger:     logger,
		}, storage.NewFileWrapperMock()),
	})
	sm = state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		PersistState: ps,
	})
	n = RaftBrainImpl{
		persistState: ps,
		stateMachine: sm, commitIndex: 3, lastApplied: 0}
	err = n.deleteLogFrom(ctx, 4)
	assert.ErrorIs(t, err, common.ErrIndexOutOfRange)
	err = n.deleteLogFrom(ctx, 0)
	assert.ErrorIs(t, err, common.ErrIndexOutOfRange)

	ps = common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
		Logs: copyData(),
		Storage: storage.NewStorageForTest(storage.NewStorageParams{
			WalSize:    1000,
			DataFolder: "data/",
			Logger:     logger,
		}, storage.NewFileWrapperMock()),
	})
	sm = state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		PersistState: ps,
	})
	n = RaftBrainImpl{
		persistState: ps,
		stateMachine: sm, commitIndex: 3, lastApplied: 0}
	err = n.deleteLogFrom(ctx, 3)
	assert.NoError(t, err)
	// assert.Equal(t, data[:2], n.logs)

	ps = common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
		Logs: copyData(),
		Storage: storage.NewStorageForTest(storage.NewStorageParams{
			WalSize:    1000,
			DataFolder: "data/",
			Logger:     logger,
		}, storage.NewFileWrapperMock()),
	})
	sm = state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		PersistState: ps,
	})
	n = RaftBrainImpl{
		persistState: ps,
		stateMachine: sm, commitIndex: 3, lastApplied: 0}
	err = n.deleteLogFrom(ctx, 2)
	assert.NoError(t, err)
	// assert.Equal(t, data[:1], n.logs)

	ps = common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
		Logs: copyData(),
		Storage: storage.NewStorageForTest(storage.NewStorageParams{
			WalSize:    1000,
			DataFolder: "data/",
			Logger:     logger,
		}, storage.NewFileWrapperMock()),
	})
	sm = state_machine.NewKeyValueStateMachine(state_machine.NewKeyValueStateMachineParams{
		PersistState: ps,
	})
	n = RaftBrainImpl{
		persistState: ps,
		stateMachine: sm, commitIndex: 3, lastApplied: 0}
	err = n.deleteLogFrom(ctx, 1)
	assert.NoError(t, err)
	// assert.Equal(t, []common.Log{}, n.logs)
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
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{},
			})},
			output: true,
		},
		{
			name:         "lastLogTerm > term",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{{Term: 1}, {Term: 2}},
			})},
			output: true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex = index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{{Term: 1}, {Term: 2}, {Term: 5}},
			})},
			output: true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex > index",
			lastLogIndex: 3,
			lastLogTerm:  5,
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{{Term: 1}, {Term: 5}},
			})},
			output: true,
		},
		{
			name:         "lastLogTerm == term && lastLogIndex < index",
			lastLogIndex: 1,
			lastLogTerm:  5,
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{{Term: 1}, {Term: 5}},
			})},
			output: false,
		},
		{
			name:         "lastLogTerm < term",
			lastLogIndex: 3,
			lastLogTerm:  3,
			n: RaftBrainImpl{persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
				Logs: []common.Log{{Term: 3}, {Term: 4}},
			})},
			output: false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.output, testCase.n.isLogUpToDate(testCase.lastLogIndex, testCase.lastLogTerm), testCase.name)
	}
}

func TestRaftBrainImpl_deleteLogFrom(t *testing.T) {
	type fields struct {
		clusterClock              *ClusterClock
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
				logger:                    tt.fields.logger,
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
				inOutLock:                 tt.fields.inOutLock,
				changeMemberLock:          tt.fields.changeMemberLock,
				dataLock:                  tt.fields.dataLock,
				lastHeartbeatReceivedTime: tt.fields.lastHeartbeatReceivedTime,
				RpcRequestTimeout:         tt.fields.RpcRequestTimeout,
				persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
					CurrentTerm: tt.fields.currentTerm,
					VotedFor:    tt.fields.votedFor,
					Logs:        tt.fields.logs,
				}),
				commitIndex: tt.fields.commitIndex,
				lastApplied: tt.fields.lastApplied,
				nextIndex:   tt.fields.nextIndex,
				matchIndex:  tt.fields.matchIndex,
			}
			if err := n.deleteLogFrom(tt.args.ctx, tt.args.index); (err != nil) != tt.wantErr {
				t.Errorf("RaftBrainImpl.deleteLogFrom() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRaftBrainImpl_GetLog(t *testing.T) {
	type fields struct {
		snapshots common.SnapshotMetadata
		logs      []common.Log
	}
	type args struct {
		index int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    common.Log
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				snapshots: common.SnapshotMetadata{},
				logs:      []common.Log{},
			},
			args: args{
				index: 1,
			},
			want:    common.Log{},
			wantErr: true,
		},
		{
			name: "with snapshot, no logs",
			fields: fields{
				snapshots: common.SnapshotMetadata{
					LastLogTerm: 4, LastLogIndex: 5,
				},
				logs: []common.Log{},
			},
			args: args{
				index: 1,
			},
			want:    common.Log{Term: 4},
			wantErr: true,
		},
		{
			name: "no snapshot",
			fields: fields{
				snapshots: common.SnapshotMetadata{},
				logs: []common.Log{
					{Term: 1, Command: "set counter 1"},
					{Term: 2, Command: "set counter 2"},
					{Term: 3, Command: "set counter 3"},
				},
			},
			args: args{
				index: 1,
			},
			want:    common.Log{Term: 1, Command: "set counter 1"},
			wantErr: false,
		},
		{
			name: "with snapshot",
			fields: fields{
				snapshots: common.SnapshotMetadata{
					LastLogTerm: 3, LastLogIndex: 3,
				},
				logs: []common.Log{
					{Term: 4, Command: "set counter 4"},
					{Term: 5, Command: "set counter 5"},
					{Term: 6, Command: "set counter 6"},
				},
			},
			args: args{
				index: 4,
			},
			want:    common.Log{Term: 4, Command: "set counter 4"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				persistState: common.NewRaftPersistanceState(common.NewRaftPersistanceStateParams{
					Logs:             tt.fields.logs,
					SnapshotMetadata: tt.fields.snapshots,
				}),
			}
			got, err := n.GetLog(tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("RaftBrainImpl.GetLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RaftBrainImpl.GetLog() = %v, want %v", got, tt.want)
			}
		})
	}
}
