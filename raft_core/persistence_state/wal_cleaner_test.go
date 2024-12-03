package persistence_state

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/storage"
	"reflect"
	"sync"
	"testing"
)

func TestRaftPersistenceStateImpl_WalCleanup(t *testing.T) {
	logger := observability.NewZerologForTest()

	type fields struct {
		latestSnapshot common.SnapshotMetadata
		storage        storage.NewStorageParams
		fileWrapper    storage.FileWrapperMock
		logger         observability.Logger
	}
	type args struct {
		sm common.SnapshotMetadata
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		wantData map[string][]string
	}{
		{
			name: "1",
			fields: fields{
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 5, LastLogIndex: 20},
				storage:        storage.NewStorageParams{WalSize: 100, DataFolder: "", Logger: logger},
				fileWrapper: storage.FileWrapperMock{
					Data: map[string][]string{
						"wal.001.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=0|0", prevWalLastLogInfoKey)},
						"wal.002.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=1|5", prevWalLastLogInfoKey)},
						"wal.003.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=4|6", prevWalLastLogInfoKey)},
						"wal.004.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=5|19", prevWalLastLogInfoKey)},
						"wal.005.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=5|20", prevWalLastLogInfoKey)},
						"wal.006.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=5|21", prevWalLastLogInfoKey)},
					},
					Size: map[string]int64{},
				},
				logger: logger,
			},
			args: args{
				sm: common.SnapshotMetadata{LastLogTerm: 5, LastLogIndex: 20},
			},
			wantErr: false,
			wantData: map[string][]string{
				"wal.005.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=5|20", prevWalLastLogInfoKey)},
				"wal.006.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=5|21", prevWalLastLogInfoKey)},
			},
		},
		{
			name: "2",
			fields: fields{
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 50},
				storage:        storage.NewStorageParams{WalSize: 100, DataFolder: "", Logger: logger},
				fileWrapper: storage.FileWrapperMock{
					Data: map[string][]string{
						"wal.001.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=0|0", prevWalLastLogInfoKey)},
					},
					Size: map[string]int64{},
				},
				logger: logger,
			},
			args: args{
				sm: common.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 50},
			},
			wantErr: false,
			wantData: map[string][]string{
				"wal.001.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=0|0", prevWalLastLogInfoKey)},
			},
		},
		{
			name: "3",
			fields: fields{
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 50},
				storage:        storage.NewStorageParams{WalSize: 100, DataFolder: "", Logger: logger},
				fileWrapper: storage.FileWrapperMock{
					Data: map[string][]string{
						"wal.001.dat": {"voted_for=0", "current_term=0", fmt.Sprintf("%s=0|0", prevWalLastLogInfoKey)},
						"wal.002.dat": {"voted_for=1", "current_term=1", fmt.Sprintf("%s=1|25", prevWalLastLogInfoKey)},
					},
					Size: map[string]int64{},
				},
				logger: logger,
			},
			args: args{
				sm: common.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 50},
			},
			wantErr: false,
			wantData: map[string][]string{
				"wal.002.dat": {"voted_for=1", "current_term=1", fmt.Sprintf("%s=1|25", prevWalLastLogInfoKey)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fw := tt.fields.fileWrapper
			s := storage.NewStorageForTest(tt.fields.storage, &fw)
			r := &RaftPersistenceStateImpl{
				latestSnapshot: tt.fields.latestSnapshot,
				storage:        s,
				lock:           sync.RWMutex{},
				logger:         tt.fields.logger,
			}
			if err := r.cleanupWal(context.Background(), tt.args.sm); (err != nil) != tt.wantErr {
				t.Errorf("RaftPersistenceStateImpl.WalCleanup() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(fw.Data, tt.wantData) {
				t.Errorf("RaftPersistenceStateImpl.WalCleanup() data = %v, want %v", tt.fields.fileWrapper.Data, tt.wantData)
			}
		})
	}
}
