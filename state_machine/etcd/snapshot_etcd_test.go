package etcd

import (
	"khanh/raft-go/common"
	. "khanh/raft-go/common/etcd"
	"reflect"
	"sort"
	"testing"
)

func TestEtcdSnapshot_ToString(t *testing.T) {
	type fields struct {
		LastConfig       map[int]common.ClusterMember
		KeyValue         []KeyValue
		SnapshotMetadata common.SnapshotMetadata
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []string
	}{
		{
			name: "first",
			fields: fields{
				LastConfig: map[int]common.ClusterMember{
					1: {ID: 1, RpcUrl: "localhost:1234", HttpUrl: "localhost:8080"},
					2: {ID: 2, RpcUrl: "localhost:1235", HttpUrl: "localhost:8081"},
					3: {ID: 3, RpcUrl: "localhost:1236", HttpUrl: "localhost:8082"},
				},
				KeyValue: []KeyValue{
					{Key: "name", Value: "khanh", ModifiedIndex: 10, CreatedIndex: 5, ExpirationTime: 0},
					{Key: "nation", Value: "vietnam", ModifiedIndex: 6, CreatedIndex: 6, ExpirationTime: 123456},
				},
				SnapshotMetadata: common.SnapshotMetadata{
					LastLogTerm:  123,
					LastLogIndex: 456,
					FileName:     "snapshot.0001.dat",
				},
			},
			wantData: []string{
				"last_log_index=456", "last_log_term=123", "member_count=3", "key_value_count=2",
				"3|localhost:8082|localhost:1236",
				"1|localhost:8080|localhost:1234",
				"2|localhost:8081|localhost:1235",
				`{"key":"name","value":"khanh","modifiedIndex":10,"createdIndex":5}`,
				`{"key":"nation","value":"vietnam","modifiedIndex":6,"createdIndex":6,"expirationTime":123456}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewEtcdSnapshot(4)
			s.LastConfig = tt.fields.LastConfig
			s.SnapshotMetadata = tt.fields.SnapshotMetadata
			for _, kv := range tt.fields.KeyValue {
				s.KeyValue.ReplaceOrInsert(kv)
				if kv.ExpirationTime > 0 {
					s.KeyExpire.ReplaceOrInsert(KeyExpire{
						Key: kv.Key, ExpirationTime: kv.ExpirationTime, CreatedIndex: kv.CreatedIndex,
					})
				}
			}
			gotData := s.ToString()

			sort.Strings(gotData)
			sort.Strings(tt.wantData)
			if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("EtcdSnapshot.ToString() = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func TestEtcdSnapshot_FromString(t *testing.T) {
	type args struct {
		data []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "first",
			args: args{
				data: []string{
					"last_log_index=456", "last_log_term=123", "member_count=3", "key_value_count=2",
					"3|localhost:8082|localhost:1236",
					"1|localhost:8080|localhost:1234",
					"2|localhost:8081|localhost:1235",
					`{"key":"name","value":"khanh","modifiedIndex":10,"createdIndex":5}`,
					`{"key":"nation","value":"vietnam","modifiedIndex":6,"createdIndex":6,"expirationTime":123456}`,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewEtcdSnapshot(4)
			if err := s.FromString(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("EtcdSnapshot.FromString() error = %v, wantErr %v", err, tt.wantErr)
			}
			data := s.ToString()
			sort.Strings(data)
			sort.Strings(tt.args.data)
			if !reflect.DeepEqual(data, tt.args.data) {
				t.Errorf("EtcdSnapshot.ToString() output = %v, want %v", data, tt.args.data)
			}
		})
	}
}

func TestEtcdSnapshot_DeleteExpiredKeys(t *testing.T) {
	type fields struct {
		keyValues  []KeyValue
		keyExpires []KeyExpire
	}
	type args struct {
		currTime uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				keyValues: []KeyValue{
					{Key: "name", ModifiedIndex: 1, CreatedIndex: 1, ExpirationTime: 0},      // no expiration
					{Key: "city", ModifiedIndex: 3, CreatedIndex: 2, ExpirationTime: 15},     // creation (2) - modification (3)
					{Key: "color", ModifiedIndex: 8, CreatedIndex: 8, ExpirationTime: 14},    // creation (6) - delete(7) - recreation(8)
					{Key: "ward", ModifiedIndex: 9, CreatedIndex: 9, ExpirationTime: 16},     // creation (9)
					{Key: "street", ModifiedIndex: 10, CreatedIndex: 10, ExpirationTime: 50}, // not expired yet
				},
				keyExpires: []KeyExpire{
					{Key: "city", ModifiedIndex: 2, CreatedIndex: 2, ExpirationTime: 10},
					{Key: "city", ModifiedIndex: 3, CreatedIndex: 2, ExpirationTime: 15},
					{Key: "word", ModifiedIndex: 4, CreatedIndex: 4, ExpirationTime: 12}, // creation (4) - deletion (5)
					{Key: "color", ModifiedIndex: 6, CreatedIndex: 6, ExpirationTime: 12},
					{Key: "color", ModifiedIndex: 8, CreatedIndex: 8, ExpirationTime: 14},
					{Key: "ward", ModifiedIndex: 9, CreatedIndex: 9, ExpirationTime: 16},
					{Key: "street", ModifiedIndex: 10, CreatedIndex: 10, ExpirationTime: 50}, // not expired yet
				},
			},
			want: fields{
				keyValues: []KeyValue{
					{Key: "name", ModifiedIndex: 1, CreatedIndex: 1, ExpirationTime: 0},      // no expiration
					{Key: "street", ModifiedIndex: 10, CreatedIndex: 10, ExpirationTime: 50}, // not expired yet
				},
				keyExpires: []KeyExpire{
					{Key: "street", ModifiedIndex: 10, CreatedIndex: 10, ExpirationTime: 50}, // not expired yet
				},
			},
			args: args{
				currTime: 20,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewEtcdSnapshot(4)
			for _, kv := range tt.fields.keyValues {
				s.KeyValue.ReplaceOrInsert(kv)
			}
			for _, ke := range tt.fields.keyExpires {
				s.KeyExpire.ReplaceOrInsert(ke)
			}
			s.DeleteExpiredKeys(tt.args.currTime)

			keyValues := []KeyValue{}
			s.KeyValue.Ascend(func(item KeyValue) bool { keyValues = append(keyValues, item); return true })
			if !reflect.DeepEqual(keyValues, tt.want.keyValues) {
				t.Errorf("s.DeleteExpiredKeys() keyValues = %v, want %v", keyValues, tt.want.keyValues)
			}

			keyExpires := []KeyExpire{}
			s.KeyExpire.Ascend(func(item KeyExpire) bool { keyExpires = append(keyExpires, item); return true })
			if !reflect.DeepEqual(keyExpires, tt.want.keyExpires) {
				t.Errorf("s.DeleteExpiredKeys() keyExpires = %v, want %v", keyExpires, tt.want.keyExpires)
			}
		})
	}
}
