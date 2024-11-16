package storage

import (
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
					{FileName: "wal.0001.dat", Size: 25},
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
				{FileName: "wal.0001.dat", Size: 51},
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
					{FileName: "wal.0001.dat", Size: 25},
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
				{FileName: "wal.0001.dat", Size: 51},
				{FileName: "wal.0002.dat", Size: 25},
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
				walSizeLimit: tt.fields.walSizeLimit,
				wals:         tt.fields.wals,
				dataFolder:   tt.fields.dataFolder,
				logger:       observability.NewSimpleLog(),
				fileUtils:    &tt.fields.f,
				walLock:      sync.RWMutex{},
				objLock:      sync.RWMutex{},
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

func TestStorageImpl_WalIterator(t *testing.T) {
	s := NewStorageForTest(NewStorageParams{
		WalSize:    100,
		DataFolder: "data/",
		Logger:     observability.NewSimpleLog(),
	}, &FileWrapperMock{
		Data: map[string][]string{
			"data/wal.0001.dat": {"num=1", "num=2"},
			"data/wal.0002.dat": {"num=3", "num=4"},
			"data/wal.0003.dat": {"num=5", "num=6"},
			"data/wal.0004.dat": {"num=7", "num=8"},
		},
		Size: map[string]int64{},
	})

	next := s.WalIterator()

	totalData := [][]string{}
	fileNames := []string{}

	for {
		data, fileName, err := next()
		assert.NoError(t, err)
		if data == nil {
			break
		}

		totalData = append(totalData, data)
		fileNames = append(fileNames, fileName)
	}

	expectedTotalData := [][]string{
		{"num", "1", "num", "2"},
		{"num", "3", "num", "4"},
		{"num", "5", "num", "6"},
		{"num", "7", "num", "8"},
	}
	expectedFileNames := []string{"wal.0001.dat", "wal.0002.dat", "wal.0003.dat", "wal.0004.dat"}
	if !reflect.DeepEqual(totalData, expectedTotalData) {
		t.Errorf("totalData = %v, want = %v", totalData, expectedTotalData)
	}
	reflect.DeepEqual(fileNames, expectedFileNames)
	if !reflect.DeepEqual(totalData, expectedTotalData) {
		t.Errorf("fileNames = %v, want = %v", fileNames, expectedFileNames)
	}
}
