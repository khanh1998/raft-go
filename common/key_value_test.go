package common

import (
	"reflect"
	"testing"
)

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
				command:     "set age 25",
				clientID:    0,
				sequenceNum: 0,
			},
			wantFields: &fields{
				data: map[string]string{
					"name": "khanh",
					"age":  "25",
				},
				clients: map[int]ClientEntry{},
			},
			wantResult: "25",
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
				command:     NoOperation,
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
				data:  tt.fields.data,
				cache: tt.fields.clients,
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
				if !reflect.DeepEqual(k.data, tt.wantFields.data) {
					t.Errorf("KeyValueStateMachine.Process() = %v, want %v", k.data, tt.wantFields.data)
				}

				if !reflect.DeepEqual(k.cache, tt.wantFields.clients) {
					t.Errorf("KeyValueStateMachine.Process() = %v, want %v", k.cache, tt.wantFields.clients)
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
				data:  tt.fields.data,
				cache: tt.fields.cache,
			}
			k.setCache(tt.args.clientID, tt.args.sequenceNum, tt.args.response)
			if !reflect.DeepEqual(k.cache, tt.want.cache) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.cache, tt.want.cache)
			}

			if !reflect.DeepEqual(k.data, tt.want.data) {
				t.Errorf("KeyValueStateMachine.setCache() = %v, want %v", k.data, tt.want.data)
			}
		})
	}
}
