package state_machine

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSnapshotFile(t *testing.T) {
	type Case struct {
		input  string
		output bool
	}

	tests := []Case{
		{"snapshot.1234567890.dat", true},
		{"snapshot.123.dat", true},
		{"snapshot.0.dat", true},
		{"snapshot.1.dat", true},
		{"snapshot.1", false},
		{"snapshot.dat", false},
		{"snapshot", false},
		{"log.1.dat", false},
	}

	for _, testcase := range tests {
		assert.Equal(t, testcase.output, common.IsSnapshotFile(testcase.input))
	}
}

func TestKeyValueStateMachine_Process(t *testing.T) {
	type fields struct {
		data                  map[string]string
		clients               map[int]common.ClientEntry
		clientSessionDuration uint64
		keyLock               map[string]int
	}
	type args struct {
		log      common.Log
		logIndex int
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult string
		wantFields *fields
		wantErr    bool
	}{
		{
			name: "get ok",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "get name"},
			},
			wantResult: "khanh",
			wantErr:    false,
		},
		{
			name: "get cache",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						LastSequenceNum: 5,
						LastResponse:    "hi you",
					},
				},
			},
			args: args{
				log: common.Log{Command: "get name", ClientID: 1, SequenceNum: 5},
			},
			wantResult: "hi you",
			wantErr:    false,
		},
		{
			name: "get cache",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						LastSequenceNum: 5,
						LastResponse:    "hi you",
					},
				},
			},
			args: args{
				log: common.Log{Command: "get name", ClientID: 1, SequenceNum: 5},
			},
			wantResult: "hi you",
			wantErr:    false,
		},
		{
			name: "old command",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						LastSequenceNum: 5,
						LastResponse:    "ok",
					},
				},
			},
			args: args{
				log: common.Log{Command: "get age", ClientID: 1, SequenceNum: 4},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "set ok",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "set address ho chi minh city"},
			},
			wantFields: &fields{
				data: map[string]string{
					"name":    "khanh",
					"address": "ho chi minh city",
				},
				clients: map[int]common.ClientEntry{},
			},
			wantResult: "ho chi minh city",
			wantErr:    false,
		},
		{
			name: "del ok",
			fields: fields{
				data: map[string]string{
					"address": "ho chi minh city",
					"name":    "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "del name"},
			},
			wantFields: &fields{
				data: map[string]string{
					"address": "ho chi minh city",
				},
				clients: map[int]common.ClientEntry{},
			},
			wantResult: "",
			wantErr:    false,
		},
		{
			name: "del failed",
			fields: fields{
				data: map[string]string{
					"address": "ho chi minh city",
					"name":    "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "del nation"},
			},
			wantFields: &fields{
				data: map[string]string{
					"address": "ho chi minh city",
					"name":    "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "set no-op",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: common.NoOperation},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			wantResult: "",
			wantErr:    false,
		},
		{
			name: "empty command",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "missing args",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "get"},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "missing args",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "set name"},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "unsupported command",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "do whatever you want", ClientID: 5, SequenceNum: 6},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "register new client",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients:               map[int]common.ClientEntry{},
				clientSessionDuration: 5,
			},
			args: args{
				logIndex: 2,
				log:      common.Log{Command: "register", ClusterTime: 3},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					2: {
						LastSequenceNum: 0,
						LastResponse:    "",
						ExpiryTime:      8,
						LockedKeys:      make(map[string]struct{}),
					},
				},
			},
			wantResult: "",
			wantErr:    false,
		},
		{
			name: "session valid",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {ExpiryTime: 20},
				},
			},
			args: args{
				log: common.Log{Command: "set address ho chi minh city", ClientID: 1, SequenceNum: 1, ClusterTime: 10},
			},
			wantFields: &fields{
				data: map[string]string{
					"name":    "khanh",
					"address": "ho chi minh city",
				},
				clients: map[int]common.ClientEntry{
					1: {ExpiryTime: 20, LastSequenceNum: 1, LastResponse: "ho chi minh city"},
				},
			},
			wantResult: "ho chi minh city",
			wantErr:    false,
		},
		{
			name: "session timeout",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {ExpiryTime: 28},
					2: {ExpiryTime: 29},
					3: {ExpiryTime: 30},
					4: {ExpiryTime: 31},
				},
			},
			args: args{
				log: common.Log{Command: "set address ho chi minh city", ClientID: 1, SequenceNum: 1, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					3: {ExpiryTime: 30},
					4: {ExpiryTime: 31},
				},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "keep-alive",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {ExpiryTime: 28},
					2: {ExpiryTime: 29},
					3: {ExpiryTime: 30},
					4: {ExpiryTime: 31},
				},
				clientSessionDuration: 5,
			},
			args: args{
				log: common.Log{Command: "keep-alive", ClientID: 3, SequenceNum: 1, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					3: {ExpiryTime: 35, LastSequenceNum: 1, LastResponse: ""},
					4: {ExpiryTime: 31},
				},
			},
			wantResult: "",
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "keep-alive" {
				fmt.Println("breakpoint")
			}
			k := KeyValueStateMachine{
				current: &common.Snapshot{
					KeyValue: tt.fields.data,
					Sessions: tt.fields.clients,
					KeyLock:  tt.fields.keyLock,
				},
				logger:                observability.NewZerolog(common.ObservabilityConfig{Disabled: true}, 0),
				clientSessionDuration: tt.fields.clientSessionDuration,
			}
			gotResult, err := k.Process(context.TODO(), tt.args.logIndex, tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
				return
			}

			if tt.wantFields != nil {
				if !reflect.DeepEqual(k.current.KeyValue, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() data = %v, want %v", k.current.KeyValue, tt.wantFields.data)
					return
				}

				if !reflect.DeepEqual(k.current.Sessions, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() session = %v, want %v", k.current.Sessions, tt.wantFields.clients)
					return
				}

				if !reflect.DeepEqual(k.current.KeyLock, tt.wantFields.keyLock) {
					t.Errorf("KeyValueStateMachine.Process() keyLock = %v, want %v", k.current.KeyLock, tt.wantFields.keyLock)
					return
				}
			}
		})
	}
}

// part 2
// test key-locking feature
func TestKeyValueStateMachine_Process2(t *testing.T) {
	type fields struct {
		data                  map[string]string
		clients               map[int]common.ClientEntry
		clientSessionDuration uint64
		keyLock               map[string]int
	}
	type args struct {
		logIndex int
		log      common.Log
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult string
		wantFields *fields
		wantErr    bool
	}{
		{
			name: "set and lock a new key",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 1, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{},
					},
				},
				clientSessionDuration: 5,
				keyLock:               map[string]int{},
			},
			args: args{
				log: common.Log{Command: "set --lock nation vietnam", ClientID: 1, SequenceNum: 2, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name":   "khanh",
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				keyLock: map[string]int{
					"nation": 1,
				},
			},
			wantResult: "vietnam",
			wantErr:    false,
		},
		{
			name: "set and lock an existing key, same client id",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 1, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{},
					},
				},
				clientSessionDuration: 5,
				keyLock:               map[string]int{},
			},
			args: args{
				log: common.Log{Command: "set --lock name khanh", ClientID: 1, SequenceNum: 2, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 2, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
				},
				keyLock: map[string]int{
					"name": 1,
				},
			},
			wantResult: "khanh",
			wantErr:    false,
		},
		{
			name: "set a locked key, different client id)",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 1, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 1, LastResponse: "",
						LockedKeys: map[string]struct{}{},
					},
				},
				clientSessionDuration: 5,
				keyLock: map[string]int{
					"name": 1,
				},
			},
			args: args{
				log: common.Log{Command: "set --lock name kelvin", ClientID: 2, SequenceNum: 2, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 1, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "",
						LockedKeys: map[string]struct{}{},
					},
				},
				keyLock: map[string]int{
					"name": 1,
				},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "set a locked key after session timeout, different client id",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 28, LastSequenceNum: 1, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 1, LastResponse: "",
						LockedKeys: map[string]struct{}{},
					},
				},
				clientSessionDuration: 5,
				keyLock: map[string]int{
					"name": 1,
				},
			},
			args: args{
				log: common.Log{Command: "set --lock name kelvin", ClientID: 2, SequenceNum: 2, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "kelvin",
				},
				clients: map[int]common.ClientEntry{
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "kelvin",
						LockedKeys: map[string]struct{}{"name": {}},
					},
				},
				keyLock: map[string]int{
					"name": 2,
				},
			},
			wantResult: "kelvin",
			wantErr:    false,
		},
		{
			name: "del locked key, same session id",
			fields: fields{
				data: map[string]string{
					"name":   "khanh",
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 2, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				clientSessionDuration: 5,
				keyLock: map[string]int{
					"name":   1,
					"nation": 2,
				},
			},
			args: args{
				log: common.Log{Command: "del name", ClientID: 1, SequenceNum: 3, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 3, LastResponse: "",
						LockedKeys: map[string]struct{}{},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				keyLock: map[string]int{
					"nation": 2,
				},
			},
			wantResult: "",
			wantErr:    false,
		},
		{
			name: "del locked key, different session id",
			fields: fields{
				data: map[string]string{
					"name":   "khanh",
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 2, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				clientSessionDuration: 5,
				keyLock: map[string]int{
					"name":   1,
					"nation": 2,
				},
			},
			args: args{
				log: common.Log{Command: "del nation", ClientID: 1, SequenceNum: 3, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name":   "khanh",
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 3, LastResponse: "",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 50, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				keyLock: map[string]int{
					"name":   1,
					"nation": 2,
				},
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "del locked key, different session id, session timeout",
			fields: fields{
				data: map[string]string{
					"name":   "khanh",
					"nation": "vietnam",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 2, LastResponse: "khanh",
						LockedKeys: map[string]struct{}{"name": {}},
					},
					2: {
						ExpiryTime: 28, LastSequenceNum: 2, LastResponse: "vietnam",
						LockedKeys: map[string]struct{}{"nation": {}},
					},
				},
				clientSessionDuration: 5,
				keyLock: map[string]int{
					"name":   1,
					"nation": 2,
				},
			},
			args: args{
				log: common.Log{Command: "del nation", ClientID: 1, SequenceNum: 3, ClusterTime: 30},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]common.ClientEntry{
					1: {
						ExpiryTime: 40, LastSequenceNum: 3, LastResponse: "",
						LockedKeys: map[string]struct{}{"name": {}},
					},
				},
				keyLock: map[string]int{
					"name": 1,
				},
			},
			wantResult: "",
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KeyValueStateMachine{
				current: &common.Snapshot{
					KeyValue: tt.fields.data,
					Sessions: tt.fields.clients,
					KeyLock:  tt.fields.keyLock,
				},
				logger:                observability.NewZerolog(common.ObservabilityConfig{Disabled: true}, 0),
				clientSessionDuration: tt.fields.clientSessionDuration,
			}
			gotResult, err := k.Process(context.TODO(), tt.args.logIndex, tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
			}

			if tt.wantFields != nil {
				if !reflect.DeepEqual(k.current.KeyValue, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() data = %v, want %v", k.current.KeyValue, tt.wantFields.data)
				}

				if !reflect.DeepEqual(k.current.Sessions, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() session = %v, want %v", k.current.Sessions, tt.wantFields.clients)
				}

				if !reflect.DeepEqual(k.current.KeyLock, tt.wantFields.keyLock) {
					t.Errorf("KeyValueStateMachine.Process() keyLock = %v, want %v", k.current.KeyLock, tt.wantFields.keyLock)
				}
			}
		})
	}
}

func TestKeyValueStateMachine_setCache(t *testing.T) {
	type fields struct {
		data  map[string]string
		cache map[int]common.ClientEntry
	}
	type args struct {
		clientID    int
		sequenceNum int
		response    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   fields
	}{
		{
			name: "both 0",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
			args: args{
				clientID:    0,
				sequenceNum: 0,
				response:    "set name khanh",
			},
			want: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
		},
		{
			name: "client 0",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
			args: args{
				clientID:    0,
				sequenceNum: 1,
				response:    "set name khanh",
			},
			want: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
		},
		{
			name: "register new client: sequence 0",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
			args: args{
				clientID:    1,
				sequenceNum: 0,
				response:    "",
			},
			want: fields{
				data: map[string]string{},
				cache: map[int]common.ClientEntry{
					1: {
						LastSequenceNum: 0,
						LastResponse:    "",
					},
				},
			},
		},
		{
			name: "ok",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]common.ClientEntry{},
			},
			args: args{
				clientID:    1,
				sequenceNum: 1,
				response:    "khanh",
			},
			want: fields{
				data: map[string]string{},
				cache: map[int]common.ClientEntry{
					1: {
						LastSequenceNum: 1,
						LastResponse:    "khanh",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KeyValueStateMachine{
				current: &common.Snapshot{
					KeyValue: tt.fields.data,
					Sessions: tt.fields.cache,
				},
			}
			k.setSession(tt.args.clientID, tt.args.sequenceNum, tt.args.response)
			if !reflect.DeepEqual(k.current.Sessions, tt.want.cache) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.current.Sessions, tt.want.cache)
			}

			if !reflect.DeepEqual(k.current.KeyValue, tt.want.data) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.current.KeyValue, tt.want.data)
			}
		})
	}
}
