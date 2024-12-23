package logic

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/common"
	"khanh/raft-go/raft_core/persistence_state"
	"khanh/raft-go/raft_core/rpc_proxy"
	"khanh/raft-go/raft_core/storage"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRaftBrainImpl_BroadCastRequestVote(t *testing.T) {
	type fields struct {
		logger              observability.Logger
		Peers               []gc.ClusterMember
		State               gc.RaftState
		ID                  int
		StateMachine        SimpleStateMachine
		ElectionTimeOut     *time.Timer
		HeartBeatTimeOut    *time.Timer
		HeartBeatTimeOutMin time.Duration
		HeartBeatTimeOutMax time.Duration
		ElectionTimeOutMin  time.Duration
		ElectionTimeOutMax  time.Duration
		RpcProxy            RPCProxy
		CurrentTerm         int
		VotedFor            int
		Logs                []gc.Log
		CommitIndex         int
		LastApplied         int
		NextIndex           map[int]int
		MatchIndex          map[int]int
	}

	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "",
			fields: fields{
				logger:              observability.NewZerolog(gc.ObservabilityConfig{}, 1),
				Peers:               []gc.ClusterMember{{ID: 2, RpcUrl: ""}},
				State:               gc.StateCandidate,
				ID:                  1,
				ElectionTimeOut:     nil,
				HeartBeatTimeOut:    nil,
				HeartBeatTimeOutMin: 8,
				HeartBeatTimeOutMax: 9,
				ElectionTimeOutMin:  40,
				ElectionTimeOutMax:  41,
				RpcProxy:            rpc_proxy.RPCProxyMock{},
				CurrentTerm:         7,
				VotedFor:            0,
				Logs:                []gc.Log{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				logger:              tt.fields.logger,
				state:               tt.fields.State,
				id:                  tt.fields.ID,
				stateMachine:        tt.fields.StateMachine,
				electionTimeOut:     tt.fields.ElectionTimeOut,
				heartBeatTimeOut:    tt.fields.HeartBeatTimeOut,
				heartBeatTimeOutMin: tt.fields.HeartBeatTimeOutMin,
				heartBeatTimeOutMax: tt.fields.HeartBeatTimeOutMax,
				electionTimeOutMin:  tt.fields.ElectionTimeOutMin,
				electionTimeOutMax:  tt.fields.ElectionTimeOutMax,
				rpcProxy:            tt.fields.RpcProxy,
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					CurrentTerm: tt.fields.CurrentTerm,
					VotedFor:    tt.fields.VotedFor,
					Logs:        tt.fields.Logs,
				}),
				commitIndex: tt.fields.CommitIndex,
				lastApplied: tt.fields.LastApplied,
				nextIndex:   tt.fields.NextIndex,
				matchIndex:  tt.fields.MatchIndex,
			}
			n.BroadCastRequestVote(context.TODO())
		})
	}
}

func TestRaftBrainImpl_BroadcastAppendEntries(t *testing.T) {
	type fields struct {
		logger                    observability.Logger
		members                   []gc.ClusterMember
		nextMemberId              int
		state                     gc.RaftState
		id                        int
		leaderID                  int
		stateMachine              SimpleStateMachine
		electionTimeOut           *time.Timer
		heartBeatTimeOut          *time.Timer
		rpcProxy                  RPCProxy
		lastHeartbeatReceivedTime time.Time
		RpcRequestTimeout         time.Duration
		logLengthLimit            int
		persistState              RaftPersistenceState
		commitIndex               int
		lastApplied               int
		nextIndex                 map[int]int
		matchIndex                map[int]int
		nextOffset                map[int]NextOffset
		snapshotChunkSize         int
		logFactory                gc.LogFactory
	}
	type args struct {
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantMajorityOK bool
		wantNextOffset map[int]NextOffset
	}{
		{
			name: "snapshot change during snapshot installing",
			fields: fields{
				id:                1,
				state:             gc.StateLeader,
				leaderID:          1,
				snapshotChunkSize: 150,
				rpcProxy: rpc_proxy.RPCProxyMock{
					InstallSnapshot: map[int]common.InstallSnapshotOutput{
						2: {
							Term:    1,
							Success: true,
							Message: "",
							NodeID:  2,
						},
					},
					Logger: observability.NewZerologForTest(),
				},
				members: []gc.ClusterMember{
					{ID: 1}, {ID: 2},
				},
				persistState: persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{
					VotedFor:    1,
					CurrentTerm: 1,
					Logs: []gc.Log{
						common.MockedLog{Term: 1, Command: "set counter 10"},
					},
					SnapshotMetadata: gc.SnapshotMetadata{
						LastLogTerm:  1,
						LastLogIndex: 9,
						FileName:     "snapshot.00000000000000000001_00000000000000000009.dat",
					},
					Storage: storage.NewStorageForTest(storage.NewStorageParams{
						WalSize:    10000,
						DataFolder: "data/",
						Logger:     observability.NewZerologForTest(),
					}, storage.FileWrapperMock{
						Data: map[string][]string{
							"data/snapshot.00000000000000000001_00000000000000000009.dat": {"some fake data for snapshot :D"},
						},
						Size: map[string]int64{},
					}),
					LogFactory: common.MockedLogFactory{},
				}),
				logger: observability.NewZerologForTest(),
				nextIndex: map[int]int{
					2: 5,
				},
				matchIndex: map[int]int{2: 0},
				nextOffset: map[int]NextOffset{
					2: {
						Offset:   450,
						FileName: "snapshot.00000000000000000001_00000000000000000005.dat",
						Snapshot: gc.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 5, FileName: "snapshot.00000000000000000001_00000000000000000005.dat"},
					},
				},
				logFactory: common.MockedLogFactory{},
			},

			args:           args{},
			wantMajorityOK: true,
			wantNextOffset: map[int]NextOffset{
				2: {
					Offset:   150,
					FileName: "snapshot.00000000000000000001_00000000000000000009.dat",
					Snapshot: gc.SnapshotMetadata{
						LastLogTerm:  1,
						LastLogIndex: 9,
						FileName:     "snapshot.00000000000000000001_00000000000000000009.dat",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{
				logger:                    tt.fields.logger,
				members:                   tt.fields.members,
				nextMemberId:              tt.fields.nextMemberId,
				state:                     tt.fields.state,
				id:                        tt.fields.id,
				leaderID:                  tt.fields.leaderID,
				stateMachine:              tt.fields.stateMachine,
				electionTimeOut:           tt.fields.electionTimeOut,
				heartBeatTimeOut:          tt.fields.heartBeatTimeOut,
				heartBeatTimeOutMin:       5 * time.Millisecond,
				heartBeatTimeOutMax:       10 * time.Millisecond,
				electionTimeOutMin:        150 * time.Millisecond,
				electionTimeOutMax:        300 * time.Millisecond,
				rpcProxy:                  tt.fields.rpcProxy,
				inOutLock:                 sync.RWMutex{},
				changeMemberLock:          sync.Mutex{},
				dataLock:                  sync.RWMutex{},
				lastHeartbeatReceivedTime: tt.fields.lastHeartbeatReceivedTime,
				RpcRequestTimeout:         tt.fields.RpcRequestTimeout,
				logLengthLimit:            tt.fields.logLengthLimit,
				persistState:              tt.fields.persistState,
				commitIndex:               tt.fields.commitIndex,
				lastApplied:               tt.fields.lastApplied,
				nextIndex:                 tt.fields.nextIndex,
				matchIndex:                tt.fields.matchIndex,
				nextOffset:                tt.fields.nextOffset,
				snapshotChunkSize:         tt.fields.snapshotChunkSize,
			}
			if gotMajorityOK := n.BroadcastAppendEntries(context.Background()); gotMajorityOK != tt.wantMajorityOK {
				t.Errorf("RaftBrainImpl.BroadcastAppendEntries() = %v, want %v", gotMajorityOK, tt.wantMajorityOK)
			}

			if !reflect.DeepEqual(n.nextOffset, tt.wantNextOffset) {
				t.Errorf("RaftBrainImpl.BroadcastAppendEntries() = %v, want %v", n.nextOffset, tt.wantNextOffset)
			}
		})
	}
}
