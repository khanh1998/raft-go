package storage

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"reflect"
	"testing"
)

func TestStorageImpl_AppendWal(t *testing.T) {
	type fields struct {
		walSizeLimit      int64
		wals              []WalMetadata
		snapshotFileNames []string
		dataFolder        string

		f FileWrapperMock
	}
	type args struct {
		metadata       []string
		keyValuesPairs []string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		wantF    FileWrapperMock
		wantWals []WalMetadata
	}{
		{
			name: "append to current wal",
			fields: fields{
				walSizeLimit: 100,
				wals: []WalMetadata{
					{FileName: "wal.0001.dat", LastLogIndex: 0, Size: 25},
				},
				snapshotFileNames: []string{},
				dataFolder:        "data/",

				f: FileWrapperMock{
					Data: map[string][]string{
						"data/wal.0001.dat": {"current_term=1", "voted_for=3"},
					},
					Size: map[string]int64{
						"data/wal.0001.dat": 25,
					},
				},
			},
			args: args{
				metadata: []string{"current_term", "1", "voted_for", "3"},
				keyValuesPairs: []string{
					"append_log", common.Log{Term: 1, Command: "set x 1"}.ToString(),
				},
			},
			wantErr: false,
			wantWals: []WalMetadata{
				{FileName: "wal.0001.dat", LastLogIndex: 0, Size: 51},
			},
			wantF: FileWrapperMock{
				Data: map[string][]string{
					"data/wal.0001.dat": {
						"current_term=1", "voted_for=3",
						fmt.Sprintf("append_log=%s", common.Log{Term: 1, Command: "set x 1"}.ToString()),
					},
				},
				Size: map[string]int64{
					"data/wal.0001.dat": 51,
				},
			},
		},
		{
			name: "wal size limit is reached, create new wal",
			fields: fields{
				walSizeLimit: 25,
				wals: []WalMetadata{
					{FileName: "wal.0001.dat", LastLogIndex: 0, Size: 25},
				},
				dataFolder: "data/",

				f: FileWrapperMock{
					Data: map[string][]string{
						"data/wal.0001.dat": {"current_term=1", "voted_for=3"},
					},
					Size: map[string]int64{
						"data/wal.0001.dat": 25,
					},
				},
			},
			args: args{
				metadata: []string{"current_term", "1", "voted_for", "3"},
				keyValuesPairs: []string{
					"append_log", common.Log{Term: 1, Command: "set x 1"}.ToString(),
				},
			},
			wantErr: false,
			wantWals: []WalMetadata{
				{FileName: "wal.0001.dat", LastLogIndex: 0, Size: 51},
				{FileName: "wal.0002.dat", LastLogIndex: 0, Size: 25},
			},
			wantF: FileWrapperMock{
				Data: map[string][]string{
					"data/wal.0001.dat": {
						"current_term=1", "voted_for=3",
						fmt.Sprintf("append_log=%s", common.Log{Term: 1, Command: "set x 1"}.ToString()),
					},
					"data/wal.0002.dat": {
						"current_term=1", "voted_for=3",
					},
				},
				Size: map[string]int64{
					"data/wal.0001.dat": 51,
					"data/wal.0002.dat": 25,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StorageImpl{
				walSizeLimit:      tt.fields.walSizeLimit,
				wals:              tt.fields.wals,
				snapshotFileNames: tt.fields.snapshotFileNames,
				dataFolder:        tt.fields.dataFolder,
				logger:            observability.NewSimpleLog(),
				fileUtils:         &tt.fields.f,
			}
			if err := s.AppendWal(tt.args.metadata, tt.args.keyValuesPairs...); (err != nil) != tt.wantErr {
				t.Errorf("StorageImpl.AppendWal() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr || !reflect.DeepEqual(tt.wantF, tt.fields.f) {
				t.Errorf("StorageImpl.AppendWal() file wrapper = %v, want %v", tt.fields.f, tt.wantF)
			}

			if tt.wantErr || !reflect.DeepEqual(tt.wantWals, s.wals) {
				t.Errorf("StorageImpl.AppendWal() wals = %v, want %v", s.wals, tt.wantWals)
			}
		})
	}
}
