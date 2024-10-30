package state_machine

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"reflect"
	"sort"
	"sync"
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
		clients               map[int]ClientEntry
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
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "set address ho chi minh city"},
			},
			wantFields: &fields{
				data: map[string]string{
					"name":    "khanh",
					"address": "ho chi minh city",
				},
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "del name"},
			},
			wantFields: &fields{
				data: map[string]string{
					"address": "ho chi minh city",
				},
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
			},
			args: args{
				log: common.Log{Command: "del nation"},
			},
			wantFields: &fields{
				data: map[string]string{
					"address": "ho chi minh city",
					"name":    "khanh",
				},
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
			},
			args: args{
				log: common.Log{Command: common.NoOperation},
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{},
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
				clients:               map[int]ClientEntry{},
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				current: snapshot{
					data:     tt.fields.data,
					sessions: tt.fields.clients,
					keyLock:  tt.fields.keyLock,
				},
				logger:                observability.NewZerolog(common.ObservabilityConfig{Disabled: true}, 0),
				clientSessionDuration: tt.fields.clientSessionDuration,
			}
			gotResult, err := k.Process(tt.args.logIndex, tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
				return
			}

			if tt.wantFields != nil {
				if !reflect.DeepEqual(k.current.data, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() data = %v, want %v", k.current.data, tt.wantFields.data)
					return
				}

				if !reflect.DeepEqual(k.current.sessions, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() session = %v, want %v", k.current.sessions, tt.wantFields.clients)
					return
				}

				if !reflect.DeepEqual(k.current.keyLock, tt.wantFields.keyLock) {
					t.Errorf("KeyValueStateMachine.Process() keyLock = %v, want %v", k.current.keyLock, tt.wantFields.keyLock)
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
		clients               map[int]ClientEntry
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				clients: map[int]ClientEntry{
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
				current: snapshot{
					data:     tt.fields.data,
					sessions: tt.fields.clients,
					keyLock:  tt.fields.keyLock,
				},
				logger:                observability.NewZerolog(common.ObservabilityConfig{Disabled: true}, 0),
				clientSessionDuration: tt.fields.clientSessionDuration,
			}
			gotResult, err := k.Process(tt.args.logIndex, tt.args.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
			}

			if tt.wantFields != nil {
				if !reflect.DeepEqual(k.current.data, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() data = %v, want %v", k.current.data, tt.wantFields.data)
				}

				if !reflect.DeepEqual(k.current.sessions, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() session = %v, want %v", k.current.sessions, tt.wantFields.clients)
				}

				if !reflect.DeepEqual(k.current.keyLock, tt.wantFields.keyLock) {
					t.Errorf("KeyValueStateMachine.Process() keyLock = %v, want %v", k.current.keyLock, tt.wantFields.keyLock)
				}
			}
		})
	}
}

func TestKeyValueStateMachine_setCache(t *testing.T) {
	type fields struct {
		data  map[string]string
		cache map[int]ClientEntry
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
				cache: map[int]ClientEntry{},
			},
			args: args{
				clientID:    0,
				sequenceNum: 0,
				response:    "set name khanh",
			},
			want: fields{
				data:  map[string]string{},
				cache: map[int]ClientEntry{},
			},
		},
		{
			name: "client 0",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]ClientEntry{},
			},
			args: args{
				clientID:    0,
				sequenceNum: 1,
				response:    "set name khanh",
			},
			want: fields{
				data:  map[string]string{},
				cache: map[int]ClientEntry{},
			},
		},
		{
			name: "register new client: sequence 0",
			fields: fields{
				data:  map[string]string{},
				cache: map[int]ClientEntry{},
			},
			args: args{
				clientID:    1,
				sequenceNum: 0,
				response:    "",
			},
			want: fields{
				data: map[string]string{},
				cache: map[int]ClientEntry{
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
				cache: map[int]ClientEntry{},
			},
			args: args{
				clientID:    1,
				sequenceNum: 1,
				response:    "khanh",
			},
			want: fields{
				data: map[string]string{},
				cache: map[int]ClientEntry{
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
				current: snapshot{
					data:     tt.fields.data,
					sessions: tt.fields.cache,
				},
			}
			k.setSession(tt.args.clientID, tt.args.sequenceNum, tt.args.response)
			if !reflect.DeepEqual(k.current.sessions, tt.want.cache) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.current.sessions, tt.want.cache)
			}

			if !reflect.DeepEqual(k.current.data, tt.want.data) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.current.data, tt.want.data)
			}
		})
	}
}

func TestKeyValueStateMachine_serializeSnapshot(t *testing.T) {
	type fields struct {
		current     snapshot
		previous    snapshot
		persistance Persistance
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "case 1",
			fields: fields{
				previous: snapshot{
					keyLock: map[string]int{
						"name": 1,
						"city": 2,
					},
					sessions: map[int]ClientEntry{
						1: {LastSequenceNum: 2, LastResponse: "abc", ExpiryTime: 100},
						2: {LastSequenceNum: 3, LastResponse: "", ExpiryTime: 130},
					},
					lastConfig: map[int]common.ClusterMember{
						1: {
							ID:      1,
							RpcUrl:  "localhost:1234",
							HttpUrl: "localhost:8080",
						},
						2: {
							ID:      2,
							RpcUrl:  "localhost:1235",
							HttpUrl: "localhost:8081",
						},
					},
					data: map[string]string{
						"name":        "khanh",
						"city":        "hcm",
						"citizenship": "vietnam",
					},

					lastTerm:  5,
					lastIndex: 6,
				},
			},
			want: []string{
				"last_log_index=6",
				"last_log_term=5",
				"member_count=2",
				"session_count=2",
				"key_value_count=3",
				"key_lock_count=2",

				"1|localhost:8080|localhost:1234",
				"2|localhost:8081|localhost:1235",

				"1|2|100|abc",
				"2|3|130|",

				"name=khanh",
				"city=hcm",
				"citizenship=vietnam",

				"name=1",
				"city=2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &KeyValueStateMachine{
				current:     tt.fields.current,
				previous:    tt.fields.previous,
				persistance: tt.fields.persistance,
			}
			got := k.serializeSnapshot()
			sort.Strings(got)
			sort.Strings(tt.want)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeyValueStateMachine.takeSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeyValueStateMachine_deserializeSnapshot(t *testing.T) {
	type fields struct {
	}
	type args struct {
		data []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    snapshot
	}{
		{
			name: "case 1",
			args: args{
				data: []string{
					"last_log_index=6",
					"last_log_term=5",
					"member_count=2",
					"session_count=2",
					"key_value_count=3",
					"key_lock_count=2",

					"1|localhost:8080|localhost:1234",
					"2|localhost:8081|localhost:1235",

					"1|2|100|abc",
					"2|3|130|",

					"name=khanh",
					"city=hcm",
					"citizenship=vietnam",

					"name=1",
					"city=2",
				},
			},
			want: snapshot{
				lastConfig: map[int]common.ClusterMember{
					1: {
						ID:      1,
						RpcUrl:  "localhost:1234",
						HttpUrl: "localhost:8080",
					},
					2: {
						ID:      2,
						RpcUrl:  "localhost:1235",
						HttpUrl: "localhost:8081",
					},
				},
				data: map[string]string{
					"name":        "khanh",
					"city":        "hcm",
					"citizenship": "vietnam",
				},
				keyLock: map[string]int{
					"name": 1,
					"city": 2,
				},
				sessions: map[int]ClientEntry{
					1: {LastSequenceNum: 2, LastResponse: "abc", ExpiryTime: 100},
					2: {LastSequenceNum: 3, LastResponse: "", ExpiryTime: 130},
				},
				lastTerm:  5,
				lastIndex: 6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &KeyValueStateMachine{}
			err := k.deserializeSnapshot(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.applySnapshot() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(k.current, tt.want) {
				t.Errorf("KeyValueStateMachine.applySnapshot() = %v, want %v", k.current, tt.want)
			}
		})
	}
}

func TestKeyValueStateMachine_RestoreFromFile(t *testing.T) {
	type fields struct {
		data []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		want    snapshot
	}{
		{
			name: "case 1",
			fields: fields{
				data: []string{
					"last_log_index=6",
					"last_log_term=5",
					"member_count=2",
					"session_count=2",
					"key_value_count=3",
					"key_lock_count=2",

					"1|localhost:8080|localhost:1234",
					"2|localhost:8081|localhost:1235",

					"1|2|100|abc",
					"2|3|130|",

					"name=khanh",
					"city=hcm",
					"citizenship=vietnam",

					"name=1",
					"city=2",
				},
			},
			want: snapshot{
				lastConfig: map[int]common.ClusterMember{
					1: {
						ID:      1,
						RpcUrl:  "localhost:1234",
						HttpUrl: "localhost:8080",
					},
					2: {
						ID:      2,
						RpcUrl:  "localhost:1235",
						HttpUrl: "localhost:8081",
					},
				},
				data: map[string]string{
					"name":        "khanh",
					"city":        "hcm",
					"citizenship": "vietnam",
				},
				lastTerm:  5,
				lastIndex: 6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := common.NewPersistenceMock()
			db.SetData(tt.fields.data)
			k := &KeyValueStateMachine{
				persistance: db,
			}
			err := k.restoreFromFile()
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.RestoreFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(k.current, tt.want) {
				t.Errorf("KeyValueStateMachine.RestoreFromFile() = %v, want %v", k.current, tt.want)
			}
		})
	}
}

func TestKeyValueStateMachine_SaveSnapshotToFile(t *testing.T) {
	type fields struct {
		current snapshot
		keys    []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		want    map[string]string
	}{
		{
			name: "case 1",
			fields: fields{
				current: snapshot{
					lastConfig: map[int]common.ClusterMember{
						1: {
							ID:      1,
							RpcUrl:  "localhost:1234",
							HttpUrl: "localhost:8080",
						},
						2: {
							ID:      2,
							RpcUrl:  "localhost:1235",
							HttpUrl: "localhost:8081",
						},
					},
					data: map[string]string{
						"name":        "khanh",
						"city":        "hcm",
						"citizenship": "vietnam",
					},
					lastTerm:  5,
					lastIndex: 6,
				},
				keys: []string{
					"log_count", "member_count", "last_index", "last_term",
					"member_0", "member_1",
					"key_0", "value_0", "key_1", "value_1", "key_2", "value_2",
				},
			},
			wantErr: false,
			want: map[string]string{
				"last_index":   "6",
				"last_term":    "5",
				"member_count": "2",
				"log_count":    "3",
				"member_0":     "1|localhost:8080|localhost:1234",
				"member_1":     "2|localhost:8081|localhost:1235",
				"key_2":        "name",
				"value_2":      "khanh",
				"key_1":        "city",
				"value_1":      "hcm",
				"key_0":        "citizenship",
				"value_0":      "vietnam",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := common.NewPersistenceMock()
			k := &KeyValueStateMachine{
				current:     tt.fields.current,
				lock:        sync.RWMutex{},
				persistance: db,
			}

			err := k.saveSnapshotToFile("")
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.SaveSnapshotToFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			data, err := db.ReadKeyValuePairsToMap(tt.fields.keys)
			if err != nil {
				t.Errorf("KeyValueStateMachine.SaveSnapshotToFile() error = %v", err)
			}

			if !reflect.DeepEqual(data, tt.want) {
				t.Errorf("KeyValueStateMachine.SaveSnapshotToFile() = %v, want %v", data, tt.want)
			}
		})
	}
}

func TestKeyValueStateMachine_findLatestSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantFileName string
		wantErr      bool
	}{
		{
			name:         "case 1",
			args:         []string{"a", "snapshot.4.dat", "tmp.snapshot.10.dat", "snapshot.9.dat", "snapshot.0.dat", "wal.dat", "snapshot.8.dat"},
			wantFileName: "snapshot.9.dat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := common.NewPersistenceMock()
			db.SetFileNames(tt.args)

			k := &KeyValueStateMachine{
				persistance: db,
			}
			gotFileName, err := k.findLatestSnapshot()
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.findLatestSnapshot() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFileName != tt.wantFileName {
				t.Errorf("KeyValueStateMachine.findLatestSnapshot() = %v, want %v", gotFileName, tt.wantFileName)
			}
		})
	}
}

func TestNewKeyValueStateMachine(t *testing.T) {
	type args struct {
		data      []string
		fileNames []string
	}
	tests := []struct {
		name    string
		args    args
		want    snapshot
		wantErr bool
	}{
		{
			name: "case 1",
			args: args{
				data: []string{
					"last_index=6",
					"last_term=5",
					"member_count=2",
					"log_count=3",
					"member_0=1|localhost:8080|localhost:1234",
					"member_1=2|localhost:8081|localhost:1235",
					"key_2=name",
					"value_2=khanh",
					"key_1=city",
					"value_1=hcm",
					"key_0=citizenship",
					"value_0=vietnam",
				},
				fileNames: []string{"snapshot.9.dat"},
			},
			want: snapshot{
				lastConfig: map[int]common.ClusterMember{
					1: {
						ID:      1,
						RpcUrl:  "localhost:1234",
						HttpUrl: "localhost:8080",
					},
					2: {
						ID:      2,
						RpcUrl:  "localhost:1235",
						HttpUrl: "localhost:8081",
					},
				},
				data: map[string]string{
					"name":        "khanh",
					"city":        "hcm",
					"citizenship": "vietnam",
				},
				lastTerm:  5,
				lastIndex: 6,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := common.NewPersistenceMock()
			db.SetData(tt.args.data)
			db.SetFileNames(tt.args.fileNames)

			got, err := NewKeyValueStateMachine(NewKeyValueStateMachineParams{
				DB:         db,
				DoSnapshot: true,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKeyValueStateMachine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.current, tt.want) {
				t.Errorf("NewKeyValueStateMachine() = %v, want %v", got.current, tt.want)
			}
		})
	}
}
