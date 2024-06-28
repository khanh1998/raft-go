package state_machine

import (
	"khanh/raft-go/common"
	"reflect"
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
		data    map[string]string
		clients map[int]ClientEntry
	}
	type args struct {
		command     any
		clientID    int
		sequenceNum int
		logIndex    int
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult any
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
				command:     "get name",
				clientID:    0,
				sequenceNum: 0,
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
				command:     "get name",
				clientID:    1,
				sequenceNum: 5,
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
				command:     "get name",
				clientID:    1,
				sequenceNum: 5,
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
				command:     "get age",
				clientID:    1,
				sequenceNum: 4,
			},
			wantResult: nil,
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
				command:     "set address ho chi minh city",
				clientID:    0,
				sequenceNum: 0,
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
			name: "set no-op",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{},
			},
			args: args{
				command:     common.NoOperation,
				clientID:    0,
				sequenceNum: 0,
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{},
			},
			wantResult: nil,
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
				command:     "",
				clientID:    0,
				sequenceNum: 0,
			},
			wantResult: nil,
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
				command:     "get",
				clientID:    0,
				sequenceNum: 0,
			},
			wantResult: nil,
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
				command:     "set name",
				clientID:    0,
				sequenceNum: 0,
			},
			wantResult: nil,
			wantErr:    true,
		},
		{
			name: "command is not string",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{},
			},
			args: args{
				command:     123,
				clientID:    0,
				sequenceNum: -1,
			},
			wantResult: nil,
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
				command:     "do whatever you want",
				clientID:    5,
				sequenceNum: 6,
			},
			wantResult: nil,
			wantErr:    true,
		},
		{
			name: "register new client",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{},
			},
			args: args{
				command:     "register",
				clientID:    0,
				sequenceNum: 0,
				logIndex:    2,
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
				},
				clients: map[int]ClientEntry{
					2: {
						LastSequenceNum: 0,
						LastResponse:    nil,
					},
				},
			},
			wantResult: nil,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KeyValueStateMachine{
				current: snapshot{
					data: tt.fields.data,
				},
				sessions: tt.fields.clients,
			}
			gotResult, err := k.Process(tt.args.clientID, tt.args.sequenceNum, tt.args.command, tt.args.logIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
			}

			if tt.wantFields != nil {
				if !reflect.DeepEqual(k.current.data, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() = %v, want %v", k.current.data, tt.wantFields.data)
				}

				if !reflect.DeepEqual(k.sessions, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() = %v, want %v", k.sessions, tt.wantFields.clients)
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
		response    any
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
				response:    nil,
			},
			want: fields{
				data: map[string]string{},
				cache: map[int]ClientEntry{
					1: {
						LastSequenceNum: 0,
						LastResponse:    nil,
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
					data: tt.fields.data,
				},
				sessions: tt.fields.cache,
			}
			k.setSession(tt.args.clientID, tt.args.sequenceNum, tt.args.response)
			if !reflect.DeepEqual(k.sessions, tt.want.cache) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.sessions, tt.want.cache)
			}

			if !reflect.DeepEqual(k.current.data, tt.want.data) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.current.data, tt.want.data)
			}
		})
	}
}

func TestKeyValueStateMachine_takeSnapshot(t *testing.T) {
	type fields struct {
		current     snapshot
		previous    snapshot
		sessions    map[int]ClientEntry
		persistance Persistance
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]string
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
			},
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
			k := &KeyValueStateMachine{
				current:     tt.fields.current,
				previous:    tt.fields.previous,
				sessions:    tt.fields.sessions,
				persistance: tt.fields.persistance,
			}
			if got := k.takeSnapshot(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeyValueStateMachine.takeSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeyValueStateMachine_prepareKeys(t *testing.T) {
	type fields struct {
		persistance Persistance
		data        []string
	}
	tests := []struct {
		name     string
		fields   fields
		wantKeys []string
		wantErr  bool
	}{
		{
			name: "case 1",
			fields: fields{
				persistance: common.NewPersistenceMock(),
				data: []string{
					"last_index=6",
					"last_term=5",
					"member_count=2",
					"log_count=3",
				},
			},
			wantKeys: []string{
				"log_count", "member_count", "last_index", "last_term",
				"member_0", "member_1",
				"key_0", "value_0", "key_1", "value_1", "key_2", "value_2",
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
			gotKeys, err := k.prepareKeys()
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.prepareKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotKeys, tt.wantKeys) {
				t.Errorf("KeyValueStateMachine.prepareKeys() = %v, want %v", gotKeys, tt.wantKeys)
			}
		})
	}
}

func TestKeyValueStateMachine_applySnapshot(t *testing.T) {
	type fields struct {
	}
	type args struct {
		data map[string]string
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
				data: map[string]string{
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
			k := &KeyValueStateMachine{}
			err := k.applySnapshot(tt.args.data)
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

			err := k.saveSnapshotToFile()
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.SaveSnapshotToFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			data, err := db.ReadNewestLog(tt.fields.keys)
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
			args:         []string{"a", "snapshot.4.dat", "snapshot.9.dat", "snapshot.0.dat", "wal.dat", "snapshot.8.dat"},
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
