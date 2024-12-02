package etcd

import (
	. "khanh/raft-go/common/etcd"
	"reflect"
	"testing"
)

func TestWatcherCache_latest_oldest(t *testing.T) {
	type fields struct {
		eventCache []EtcdResultRes
		capacity   int
		baseIndex  int
	}
	tests := []struct {
		name       string
		fields     fields
		wantLatest EtcdResultRes
		wantOldest EtcdResultRes
		wantOk     bool
	}{
		{
			name: "empty",
			fields: fields{
				eventCache: []EtcdResultRes{},
				baseIndex:  0,
				capacity:   4,
			},
			wantLatest: EtcdResultRes{},
			wantOldest: EtcdResultRes{},
			wantOk:     false,
		},
		{
			name: "single",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				},
				baseIndex: 0,
				capacity:  1,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			wantOk:     true,
		},
		{
			name: "double",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
				},
				baseIndex: 0,
				capacity:  2,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			wantOk:     true,
		},
		{
			name: "not full",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			wantOk:     true,
		},
		{
			name: "full 1",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				},
				baseIndex: 0,
				capacity:  4,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			wantOk:     true,
		},
		{
			name: "full 2",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				},
				baseIndex: 1,
				capacity:  4,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
			wantOk:     true,
		},
		{
			name: "full 3",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				},
				baseIndex: 2,
				capacity:  4,
			},
			wantLatest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
			wantOldest: EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
			wantOk:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatcherCache(tt.fields.capacity)
			w.eventCache = append(w.eventCache, tt.fields.eventCache...)
			w.baseIndex = tt.fields.baseIndex

			latest, gotOk := w.latest()
			if !reflect.DeepEqual(latest, tt.wantLatest) {
				t.Errorf("WatcherCache.latest() latest = %v, want %v", latest, tt.wantLatest)
			}
			if gotOk != tt.wantOk {
				t.Errorf("WatcherCache.latest() gotOk = %v, want %v", gotOk, tt.wantOk)
			}

			oldest, gotOk := w.oldest()
			if !reflect.DeepEqual(oldest, tt.wantOldest) {
				t.Errorf("WatcherCache.latest() oldest = %v, want %v", oldest, tt.wantOldest)
			}
			if gotOk != tt.wantOk {
				t.Errorf("WatcherCache.latest() gotOk = %v, want %v", gotOk, tt.wantOk)
			}

		})
	}
}

func TestWatcherCache_find_waitIndex(t *testing.T) {
	type fields struct {
		eventCache []EtcdResultRes
		baseIndex  int
	}
	type args struct {
		key       string
		waitIndex int
		capacity  int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   EtcdResultRes
		want1  bool
	}{
		{
			name: "empty",
			fields: fields{
				eventCache: []EtcdResultRes{},
				baseIndex:  0,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 7},
			want:  EtcdResultRes{},
			want1: false,
		},
		{
			name: "waitIndex = 0",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
				},
				baseIndex: 0,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 0},
			want:  EtcdResultRes{},
			want1: false,
		},
		{
			name: "not full, found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2", CreatedIndex: 5, ModifiedIndex: 10}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 13}},
				},
				baseIndex: 4,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 7},
			want:  EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "2", CreatedIndex: 5, ModifiedIndex: 10}},
			want1: true,
		},
		{
			name: "not full, found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2", CreatedIndex: 5, ModifiedIndex: 10}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 13}},
				},
				baseIndex: 4,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 5},
			want:  EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
			want1: true,
		},
		{
			name: "not full, not found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2", CreatedIndex: 5, ModifiedIndex: 10}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 13}},
				},
				baseIndex: 4,
			},
			args:  args{key: "name", capacity: 4, waitIndex: 7},
			want:  EtcdResultRes{},
			want1: false,
		},
		{
			name: "full, found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5", CreatedIndex: 5, ModifiedIndex: 12}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6", CreatedIndex: 5, ModifiedIndex: 18}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 5}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
				},
				baseIndex: 2,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 6},
			want:  EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
			want1: true,
		},
		{
			name: "full, found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5", CreatedIndex: 5, ModifiedIndex: 12}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6", CreatedIndex: 5, ModifiedIndex: 18}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 5}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
				},
				baseIndex: 2,
			},
			args:  args{key: "count", capacity: 4, waitIndex: 11},
			want:  EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "5", CreatedIndex: 5, ModifiedIndex: 12}},
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatcherCache(tt.args.capacity)
			w.eventCache = append(w.eventCache, tt.fields.eventCache...)
			w.baseIndex = tt.fields.baseIndex
			got, got1 := w.find(tt.args.key, tt.args.waitIndex)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WatcherCache.find() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("WatcherCache.find() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestWatcherCache_Get(t *testing.T) {
	type fields struct {
		eventCache []EtcdResultRes
		baseIndex  int
	}
	type args struct {
		key       string
		waitIndex int
		capacity  int
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      EtcdResultRes
		wantFound bool
		wantErr   bool
	}{
		{
			name: "empty",
			fields: fields{
				eventCache: []EtcdResultRes{},
				baseIndex:  0,
			},
			args: args{
				key:       "count",
				waitIndex: 0,
				capacity:  4,
			},
			want:      EtcdResultRes{},
			wantFound: false,
			wantErr:   false,
		},
		{
			name: "waitIndex = 0",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
				},
				baseIndex: 0,
			},
			args: args{
				key:       "count",
				waitIndex: 0,
				capacity:  4,
			},
			want:      EtcdResultRes{},
			wantFound: false,
			wantErr:   false,
		},
		{
			name: "cache cleared",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1", CreatedIndex: 5, ModifiedIndex: 6}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2", CreatedIndex: 5, ModifiedIndex: 7}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 8}},
				},
				baseIndex: 0,
			},
			args: args{
				key:       "count",
				waitIndex: 2,
				capacity:  4,
			},
			want:      EtcdResultRes{},
			wantFound: false,
			wantErr:   true,
		},
		{
			name: "found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5", CreatedIndex: 5, ModifiedIndex: 13}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6", CreatedIndex: 5, ModifiedIndex: 17}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 5}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
				},
				baseIndex: 2,
			},
			args: args{
				key:       "count",
				waitIndex: 6,
				capacity:  4,
			},
			want:      EtcdResultRes{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
			wantFound: true,
			wantErr:   false,
		},
		{
			name: "not found",
			fields: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5", CreatedIndex: 5, ModifiedIndex: 13}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6", CreatedIndex: 5, ModifiedIndex: 17}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3", CreatedIndex: 5, ModifiedIndex: 5}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4", CreatedIndex: 5, ModifiedIndex: 9}},
				},
				baseIndex: 2,
			},
			args: args{
				key:       "name",
				waitIndex: 6,
				capacity:  4,
			},
			want:      EtcdResultRes{},
			wantFound: false,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatcherCache(tt.args.capacity)
			w.baseIndex = tt.fields.baseIndex
			w.eventCache = append(w.eventCache, tt.fields.eventCache...)
			got, found, err := w.Get(tt.args.key, tt.args.waitIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("WatcherCache.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if found != tt.wantFound {
				t.Errorf("WatcherCache.Get() found = %v, wantFound %v", found, tt.wantFound)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WatcherCache.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWatcherCache_Put(t *testing.T) {
	type fields struct {
		eventCache []EtcdResultRes
		baseIndex  int
	}
	tests := []struct {
		name     string
		input    []EtcdResultRes
		capacity int
		want     fields
	}{
		{
			name: "not full",
			input: []EtcdResultRes{
				{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
			},
			capacity: 4,
			want: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				},
				baseIndex: 0,
			},
		},
		{
			name: "full 1",
			input: []EtcdResultRes{
				{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
			},
			capacity: 4,
			want: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				},
				baseIndex: 0,
			},
		},
		{
			name: "full 2",
			input: []EtcdResultRes{
				{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
			},
			capacity: 4,
			want: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				},
				baseIndex: 1,
			},
		},
		{
			name: "full 2",
			input: []EtcdResultRes{
				{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "7"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "8"}},
			},
			capacity: 4,
			want: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "7"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "8"}},
				},
				baseIndex: 0,
			},
		},
		{
			name: "full 3",
			input: []EtcdResultRes{
				{Action: "set", Node: KeyValue{Key: "count", Value: "1"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "2"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "3"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "4"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "5"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "7"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "8"}},
				{Action: "set", Node: KeyValue{Key: "count", Value: "9"}},
			},
			capacity: 4,
			want: fields{
				eventCache: []EtcdResultRes{
					{Action: "set", Node: KeyValue{Key: "count", Value: "9"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "6"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "7"}},
					{Action: "set", Node: KeyValue{Key: "count", Value: "8"}},
				},
				baseIndex: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWatcherCache(tt.capacity)
			for _, ec := range tt.input {
				w.Put(ec)
			}

			if w.baseIndex != tt.want.baseIndex {
				t.Errorf("WatcherCache.Put() baseIndex = %v, want %v", w.baseIndex, tt.want.baseIndex)
			}

			if !reflect.DeepEqual(w.eventCache, tt.want.eventCache) {
				t.Errorf("WatcherCache.Put() eventCache = %v, want %v", w.eventCache, tt.want.eventCache)
			}
		})
	}
}
