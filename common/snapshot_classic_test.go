package common

import (
	"reflect"
	"sort"
	"testing"
)

func TestSnapshot_FromString(t *testing.T) {
	type fields struct {
		LastConfig       map[int]ClusterMember
		KeyValue         map[string]string
		KeyLock          map[string]int
		Sessions         map[int]ClientEntry
		SnapshotMetadata SnapshotMetadata
	}
	type args struct {
		data []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				LastConfig: map[int]ClusterMember{
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
				KeyValue: map[string]string{
					"name":        "khanh",
					"city":        "hcm",
					"citizenship": "vietnam",
				},
				KeyLock: map[string]int{
					"name": 1,
					"city": 2,
				},
				Sessions: map[int]ClientEntry{
					1: {LastSequenceNum: 2, LastResponse: "abc", ExpiryTime: 100},
					2: {LastSequenceNum: 3, LastResponse: "", ExpiryTime: 130},
				},
				SnapshotMetadata: SnapshotMetadata{
					LastLogTerm:  5,
					LastLogIndex: 6,
				},
			},
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
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewClassicSnapshot()
			if err := s.FromString(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Snapshot.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSnapshot_ToString(t *testing.T) {
	type fields struct {
		LastConfig       map[int]ClusterMember
		KeyValue         map[string]string
		KeyLock          map[string]int
		Sessions         map[int]ClientEntry
		SnapshotMetadata SnapshotMetadata
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []string
	}{
		{
			name: "",
			fields: fields{
				KeyLock: map[string]int{
					"name": 1,
					"city": 2,
				},
				LastConfig: map[int]ClusterMember{
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
				Sessions: map[int]ClientEntry{
					1: {LastSequenceNum: 2, LastResponse: "abc", ExpiryTime: 100},
					2: {LastSequenceNum: 3, LastResponse: "", ExpiryTime: 130},
				},
				KeyValue: map[string]string{
					"name":        "khanh",
					"city":        "hcm",
					"citizenship": "vietnam",
				},
				SnapshotMetadata: SnapshotMetadata{
					LastLogTerm:  5,
					LastLogIndex: 6,
				},
			},
			wantData: []string{
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
			s := ClassicSnapshot{
				LastConfig:       tt.fields.LastConfig,
				KeyValue:         tt.fields.KeyValue,
				KeyLock:          tt.fields.KeyLock,
				Sessions:         tt.fields.Sessions,
				SnapshotMetadata: tt.fields.SnapshotMetadata,
			}
			gotData := s.ToString()
			sort.Strings(gotData)
			sort.Strings(tt.wantData)
			if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("Snapshot.Serialize() = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}
