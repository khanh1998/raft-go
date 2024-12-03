package state_machine

import (
	"khanh/raft-go/extensions/etcd/common"
	"reflect"
	"sync"
	"testing"
)

func TestWatcher_Register(t *testing.T) {
	type fields struct {
		keyWatch    map[string]map[int]WatchRequest
		prefixWatch map[string]map[int]WatchRequest

		eventCache []common.EtcdResultRes
		baseIndex  int
		capacity   int
	}
	type args struct {
		key       string
		isPrefix  bool
		waitIndex int
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantId      int
		wantErr     bool
		wantChannel bool
		want        fields
	}{
		{
			name: "no wait",
			args: args{
				key: "count", isPrefix: false, waitIndex: 0,
			},
			fields: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
				eventCache:  []common.EtcdResultRes{},
				baseIndex:   0,
				capacity:    4,
			},
			wantId:      1,
			wantChannel: false,
			wantErr:     false,
			want: fields{
				keyWatch: map[string]map[int]WatchRequest{
					"count": {
						1: {WaitIndex: 0, Channel: make(chan common.EtcdResultRes, 1)},
					},
				},
				prefixWatch: map[string]map[int]WatchRequest{},
			},
		},
		{
			name: "cached",
			args: args{
				key: "count", isPrefix: false, waitIndex: 5,
			},
			fields: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
				eventCache: []common.EtcdResultRes{
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 4}},
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 6}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantId:      0,
			wantChannel: true,
			wantErr:     false,
			want: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
			},
		},
		{
			name: "cache cleared",
			args: args{
				key: "count", isPrefix: false, waitIndex: 3,
			},
			fields: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
				eventCache: []common.EtcdResultRes{
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 4}},
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 6}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantId:      0,
			wantChannel: false,
			wantErr:     true,
			want: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
			},
		},
		{
			name: "not found in cache",
			args: args{
				key: "name", isPrefix: false, waitIndex: 4,
			},
			fields: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
				eventCache: []common.EtcdResultRes{
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 4}},
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 6}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantId:      1,
			wantChannel: false,
			wantErr:     false,
			want: fields{
				keyWatch: map[string]map[int]WatchRequest{
					"name": {
						1: {WaitIndex: 4, Channel: make(chan common.EtcdResultRes, 1)},
					},
				},
				prefixWatch: map[string]map[int]WatchRequest{},
			},
		},
		{
			name: "prefix, not found in cache",
			args: args{
				key: "name", isPrefix: true, waitIndex: 4,
			},
			fields: fields{
				keyWatch:    map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{},
				eventCache: []common.EtcdResultRes{
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 4}},
					{Action: "set", Node: common.KeyValue{Key: "count", CreatedIndex: 4, ModifiedIndex: 6}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantId:      1,
			wantChannel: false,
			wantErr:     false,
			want: fields{
				keyWatch: map[string]map[int]WatchRequest{},
				prefixWatch: map[string]map[int]WatchRequest{
					"name": {
						1: {WaitIndex: 4, Channel: make(chan common.EtcdResultRes, 1)},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := Watcher{
				keyWatch:    tt.want.keyWatch,
				prefixWatch: tt.want.prefixWatch,
				cache: WatcherCache{
					eventCache: make([]common.EtcdResultRes, 0, tt.fields.capacity),
					baseIndex:  tt.want.baseIndex,
				},
			}
			w.cache.eventCache = append(w.cache.eventCache, tt.fields.eventCache...)

			gotId, gotChannel, err := w.Register(tt.args.key, tt.args.isPrefix, tt.args.waitIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("Watcher.Register() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotId != tt.wantId {
				t.Errorf("Watcher.Register() gotId = %v, want %v", gotId, tt.wantId)
			}
			if tt.wantChannel && len(gotChannel) == 0 {
				t.Errorf("Watcher.Register() gotChannel = %v, want %v", len(gotChannel), tt.wantChannel)
			}
		})
	}
}

func TestWatcher_Notify(t *testing.T) {
	type fields struct {
		keyWatch    map[string]map[int]WatchRequest
		prefixWatch map[string]map[int]WatchRequest
	}
	type args struct {
		newData common.EtcdResultRes
	}
	fakeChannel := make(chan common.EtcdResultRes, 10)
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        fields
		notifyCount int
	}{
		{
			name: "",
			fields: fields{
				keyWatch: map[string]map[int]WatchRequest{
					"count": {
						1: {WaitIndex: 0, Channel: fakeChannel},
						2: {WaitIndex: 6, Channel: fakeChannel},
						3: {WaitIndex: 7, Channel: fakeChannel},
					},
				},
				prefixWatch: map[string]map[int]WatchRequest{
					"counting": {},
					"count": {
						4: {WaitIndex: 0, Channel: fakeChannel},
					},
					"coun": {
						5: {WaitIndex: 6, Channel: fakeChannel},
					},
					"cou": {
						6: {WaitIndex: 7, Channel: fakeChannel},
					},
					"co": {},
					"c":  {},
				},
			},
			args: args{
				newData: common.EtcdResultRes{
					Action:   "set",
					Node:     common.KeyValue{Key: "count", CreatedIndex: 5, ModifiedIndex: 6},
					PrevNode: common.KeyValue{Key: "count", CreatedIndex: 5, ModifiedIndex: 5},
				},
			},
			want: fields{
				keyWatch: map[string]map[int]WatchRequest{
					"count": {
						3: {WaitIndex: 7, Channel: fakeChannel},
					},
				},
				prefixWatch: map[string]map[int]WatchRequest{
					"counting": {},
					"cou": {
						6: {WaitIndex: 7, Channel: fakeChannel},
					},
				},
			},
			notifyCount: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Watcher{
				keyWatch:    tt.fields.keyWatch,
				prefixWatch: tt.fields.prefixWatch,
				lock:        sync.RWMutex{},
				cache:       NewWatcherCache(4),
			}

			w.Notify(tt.args.newData)

			if !reflect.DeepEqual(w.keyWatch, tt.want.keyWatch) {
				t.Errorf("Watcher.Notify() keyWatch = %v, want %v", w.keyWatch, tt.want.keyWatch)
			}

			if !reflect.DeepEqual(w.prefixWatch, tt.want.prefixWatch) {
				t.Errorf("Watcher.Notify() prefixWatch = %v, want %v", w.prefixWatch, tt.want.prefixWatch)
			}

			if len(fakeChannel) != tt.notifyCount {
				t.Errorf("Watcher.Notify() notify count = %v, want %v", len(fakeChannel), tt.notifyCount)
			}

			// drain the channel for next test case
		OuterLoop:
			for {
				select {
				case <-fakeChannel:
				default:
					break OuterLoop
				}
			}
		})
	}
}
