package state_machine

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/persistence_state"
	"reflect"
	"sync"
	"testing"
)

func TestBtreeKvStateMachine_get(t *testing.T) {
	type fields struct {
		persistenceState RaftPersistenceState
		keyValues        []common.KeyValue
	}
	type args struct {
		log      common.EtcdLog
		logIndex int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantRes common.EtcdResultRes
		wantErr bool
	}{
		{
			name: "no wait",
			fields: fields{
				keyValues: []common.KeyValue{
					{Key: "name", Value: "khanh", CreatedIndex: 1, ModifiedIndex: 1},
				},
			},
			args: args{
				log: common.EtcdLog{
					Term: 0,
					Time: 0,
					Command: common.EtcdCommand{
						Action: "get",
						Key:    "name",
					},
				},
				logIndex: 0,
			},
			wantRes: common.EtcdResultRes{Action: "get", Node: common.KeyValue{
				Key: "name", Value: "khanh", CreatedIndex: 1, ModifiedIndex: 1,
			},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BtreeKvStateMachine{
				current:          NewEtcdSnapshot(5),
				lock:             sync.RWMutex{},
				logger:           nil,
				snapshotLock:     sync.Mutex{},
				persistenceState: tt.fields.persistenceState,
				watcher:          NewWatcher(5),
			}
			for _, kv := range tt.fields.keyValues {
				b.current.Insert(kv)
			}

			gotRes, gotErr := b.get(tt.args.log.Command, tt.args.logIndex)
			if !reflect.DeepEqual(gotRes, tt.wantRes) {
				t.Errorf("BtreeKvStateMachine.get() gotRes = %v, want %v", gotRes, tt.wantRes)
			}
			if (gotErr != nil) != tt.wantErr {
				t.Errorf("BtreeKvStateMachine.get() gotErr = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func TestBtreeKvStateMachine_Process(t *testing.T) {
	type fields struct {
		current *EtcdSnapshot
	}
	type args struct {
		logIndex int
		logI     gc.Log
	}
	rps := persistence_state.NewRaftPersistenceState(persistence_state.NewRaftPersistenceStateParams{})
	logger := observability.NewOtelLogger()
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult common.EtcdResult
		wantErr    bool
	}{
		{
			name: "1",
			fields: fields{
				current: NewEtcdSnapshot(4),
			},
			args: args{
				logIndex: 1,
				logI: common.EtcdLog{
					Term: 1,
					Time: 10,
					Command: common.EtcdCommand{
						Action: "put",
						Key:    "name",
						Value:  gc.GetPointer("khanh"),
					},
				},
			},
			wantResult: common.EtcdResult{
				Data: common.EtcdResultRes{
					Action: "set",
					Node: common.KeyValue{
						Key:           "name",
						Value:         "khanh",
						ModifiedIndex: 1,
						CreatedIndex:  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "1",
			fields: fields{
				current: NewEtcdSnapshot(4),
			},
			args: args{
				logIndex: 1,
				logI: common.EtcdLog{
					Term: 1,
					Time: 10,
					Command: common.EtcdCommand{
						Action: "get",
						Key:    "name",
						Wait:   true,
					},
				},
			},
			wantResult: common.EtcdResult{
				Data:    common.EtcdResultRes{},
				Promise: make(common.EtcdResultPromise),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BtreeKvStateMachine{
				current:          tt.fields.current,
				lock:             sync.RWMutex{},
				logger:           logger,
				snapshotLock:     sync.Mutex{},
				persistenceState: rps,
				watcher:          NewWatcher(100),
			}
			gotResult, err := b.Process(context.Background(), tt.args.logIndex, tt.args.logI)
			if (err != nil) != tt.wantErr {
				t.Errorf("BtreeKvStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantResult.Promise == nil {
				tmp := gotResult.(common.EtcdResult).Data
				if !reflect.DeepEqual(tmp, tt.wantResult.Data) {
					t.Errorf("BtreeKvStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
				}
			}
		})
	}
}
