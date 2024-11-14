package common

import (
	"reflect"
	"sort"
	"testing"
)

func TestSnapshot_Deserialize(t *testing.T) {
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
			s := NewSnapshot()
			if err := s.Deserialize(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Snapshot.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientEntry_FromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name         string
		args         args
		want         ClientEntry
		wantClientId int
		wantErr      bool
	}{
		{
			name: "1",
			args: args{
				str: "1|2|100|abc",
			},
			wantClientId: 1,
			want: ClientEntry{
				LastSequenceNum: 2,
				ExpiryTime:      100,
				LastResponse:    "abc",
			},
			wantErr: false,
		},
		{
			name: "2",
			args: args{
				str: "1|2|100|",
			},
			wantClientId: 1,
			want: ClientEntry{
				LastSequenceNum: 2,
				ExpiryTime:      100,
				LastResponse:    "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientEntry{}
			gotClientId, err := c.FromString(tt.args.str)
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientEntry.FromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotClientId != tt.wantClientId {
				t.Errorf("ClientEntry.FromString() = %v, want %v", gotClientId, tt.wantClientId)
			}

			if !reflect.DeepEqual(*c, tt.want) {
				t.Errorf("ClientEntry.FromString() = %v, want %v", *c, tt.want)
			}
		})
	}
}

func TestSnapshotMetadata_FromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    SnapshotMetadata
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				str: "snapshot.1001.dat|5|6",
			},
			want: SnapshotMetadata{
				LastLogIndex: 5,
				LastLogTerm:  6,
				FileName:     "snapshot.1001.dat",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SnapshotMetadata{}
			if err := s.FromString(tt.args.str); (err != nil) != tt.wantErr {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(*s, tt.want) {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", *s, tt.want)
			}
		})
	}
}

func TestSnapshot_Serialize(t *testing.T) {
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
			s := Snapshot{
				LastConfig:       tt.fields.LastConfig,
				KeyValue:         tt.fields.KeyValue,
				KeyLock:          tt.fields.KeyLock,
				Sessions:         tt.fields.Sessions,
				SnapshotMetadata: tt.fields.SnapshotMetadata,
			}
			gotData := s.Serialize()
			if !reflect.DeepEqual(sort.StringSlice(gotData), sort.StringSlice(tt.wantData)) {
				t.Errorf("Snapshot.Serialize() = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func TestIsSnapshotFile(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "1",
			args: args{fileName: "snapshot.1_1.dat"},
			want: true,
		},
		{
			name: "2",
			args: args{fileName: "snapshot.11.dat"},
			want: false,
		},
		{
			name: "3",
			args: args{fileName: "snapshot.a_1.dat"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSnapshotFile(tt.args.fileName); got != tt.want {
				t.Errorf("IsSnapshotFile() = %v, want %v", got, tt.want)
			}
		})
	}
}
