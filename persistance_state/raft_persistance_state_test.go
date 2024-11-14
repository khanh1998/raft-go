package persistance_state

import (
	"errors"
	"khanh/raft-go/common"
	"reflect"
	"testing"
)

func TestRaftPersistanceState_TrimPrefixLog(t *testing.T) {
	type fields struct {
		Logs []common.Log
	}
	type args struct {
		prev common.SnapshotMetadata
		curr common.SnapshotMetadata
	}
	type subCase struct {
		args args
		want []common.Log
	}
	tests := []struct {
		name     string
		fields   fields
		subcases []subCase
	}{
		{
			name: "",
			fields: fields{
				Logs: []common.Log{
					{Term: 1, Command: "set counter 1"},
					{Term: 2, Command: "set counter 2"},
					{Term: 3, Command: "set counter 3"},
					{Term: 4, Command: "set counter 4"},
				},
			},
			subcases: []subCase{
				{
					args: args{
						prev: common.SnapshotMetadata{},
						curr: common.SnapshotMetadata{
							LastLogTerm:  1,
							LastLogIndex: 1,
						},
					},
					want: []common.Log{
						{Term: 2, Command: "set counter 2"},
						{Term: 3, Command: "set counter 3"},
						{Term: 4, Command: "set counter 4"},
					},
				},
				{
					args: args{
						prev: common.SnapshotMetadata{},
						curr: common.SnapshotMetadata{
							LastLogTerm:  3,
							LastLogIndex: 3,
						},
					},
					want: []common.Log{
						{Term: 4, Command: "set counter 4"},
					},
				},
				{
					args: args{
						prev: common.SnapshotMetadata{},
						curr: common.SnapshotMetadata{
							LastLogTerm:  4,
							LastLogIndex: 4,
						},
					},
					want: []common.Log{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, sub := range tt.subcases {
				r := &RaftPersistanceStateImpl{
					logs: common.CopySlice(tt.fields.Logs),
				}

				r.trimPrefixLog(sub.args.curr)

				if !reflect.DeepEqual(r.logs, sub.want) {
					t.Errorf("logs are not equal, got %v, want %v", r.logs, sub.want)
				}
			}
		})
	}
}

func TestRaftPersistanceState_Deserialize(t *testing.T) {
	type args struct {
		data           []string
		latestSnapshot common.SnapshotMetadata
	}
	tests := []struct {
		name             string
		raft             RaftPersistanceStateImpl
		args             args
		want             RaftPersistanceStateImpl
		wantLastLogIndex int
		wantErr          bool
	}{
		{
			name: "no log",
			raft: RaftPersistanceStateImpl{},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
				},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs:        nil,
			}),
			wantLastLogIndex: 0,
			wantErr:          false,
		},
		{
			name: "with logs",
			raft: RaftPersistanceStateImpl{},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
					"last_log_info", "0|1",
					"append_log", common.Log{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
				},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2},
					{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1},
				},
			}),
			wantLastLogIndex: 2,
			wantErr:          false,
		},
		{
			name: "with logs, with previous state",
			raft: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				VotedFor:    4,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 1", ClientID: 1, SequenceNum: 2},
					{Term: 2, Command: "set y 2", ClientID: 2, SequenceNum: 1},
				},
			}),
			args: args{
				data: []string{
					"current_term", "4",
					"voted_for", "2",
					"last_log_info", "2|2",
					"append_log", common.Log{Term: 3, Command: "set y 3", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 4, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 4, Command: "set y 5", ClientID: 5, SequenceNum: 2}.ToString(),
					"delete_log", "1",
				},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 4,
				VotedFor:    2,
				Logs: []common.Log{
					{Term: 1, Command: "set x 1", ClientID: 1, SequenceNum: 2},
					{Term: 2, Command: "set y 2", ClientID: 2, SequenceNum: 1},
					{Term: 3, Command: "set y 3", ClientID: 3, SequenceNum: 2},
					{Term: 4, Command: "set y 4", ClientID: 5, SequenceNum: 1},
				},
			}),
			wantLastLogIndex: 4,
			wantErr:          false,
		},
		{
			name: "with logs, with snapshot 001",
			raft: RaftPersistanceStateImpl{},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
					"last_log_info", "0|1",
					"append_log", common.Log{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
				},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 2, LastLogIndex: 2, FileName: "snapshot.001.dat"},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs:        []common.Log{},
			}),
			wantLastLogIndex: 2,
			wantErr:          false,
		},
		{
			name: "with logs, with snapshot 002",
			raft: RaftPersistanceStateImpl{},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
					"last_log_info", "0|1",
					"append_log", common.Log{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
					"append_log", common.Log{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2}.ToString(),
				},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 2, LastLogIndex: 2, FileName: "snapshot.001.dat"},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs: []common.Log{
					{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1},
					{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2},
				},
			}),
			wantLastLogIndex: 4,
			wantErr:          false,
		},
		{
			name: "with logs, with snapshot, with previous logs 001",
			raft: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				VotedFor:    4,
				CurrentTerm: 3,
				Logs: []common.Log{
					{Term: 1, Command: "set y 3", ClientID: 2, SequenceNum: 2},
				},
			}),
			args: args{
				data: []string{
					"current_term", "5",
					"voted_for", "1",
					"last_log_info", "3|1",
					"append_log", common.Log{Term: 1, Command: "set y 2", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
					"append_log", common.Log{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2}.ToString(),
				},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 1, LastLogIndex: 2, FileName: "snapshot.001.dat"},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 5,
				VotedFor:    1,
				Logs: []common.Log{
					{Term: 1, Command: "set y 3", ClientID: 2, SequenceNum: 2},
					{Term: 1, Command: "set y 2", ClientID: 3, SequenceNum: 2},
					{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1},
					{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1},
					{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2},
				},
			}),
			wantLastLogIndex: 7,
			wantErr:          false,
		},
		{
			name: "with logs, with snapshot, with previous logs 002",
			raft: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				VotedFor:    4,
				CurrentTerm: 3,
				Logs:        []common.Log{}, // logs are deleted from previous run
			}),
			args: args{
				data: []string{
					"current_term", "5",
					"voted_for", "1",
					"last_log_info", "3|1",
					"append_log", common.Log{Term: 1, Command: "set y 2", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
					"append_log", common.Log{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2}.ToString(),
				},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 2, LastLogIndex: 5, FileName: "snapshot.001.dat"},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 5,
				VotedFor:    1,
				Logs: []common.Log{
					{Term: 3, Command: "set y 5", ClientID: 6, SequenceNum: 1},
					{Term: 3, Command: "set y 6", ClientID: 6, SequenceNum: 2},
				},
			}),
			wantLastLogIndex: 7,
			wantErr:          false,
		},
		{
			name: "with logs, with snapshot, with previous logs, with previous snapshot 001",
			raft: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				VotedFor:    4,
				CurrentTerm: 3,
				Logs:        []common.Log{}, // log are deleted from previous run
			}),
			args: args{
				data: []string{
					"current_term", "6",
					"voted_for", "1",
					"last_log_info", "6|3",
					"append_log", common.Log{Term: 4, Command: "set y 7", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 4, Command: "set y 8", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 5, Command: "set y 9", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
					"append_log", common.Log{Term: 6, Command: "set y 10", ClientID: 6, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 6, Command: "set y 11", ClientID: 6, SequenceNum: 2}.ToString(),
				},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 4, LastLogIndex: 8, FileName: "snapshot.002.dat"},
			},
			want: *NewRaftPersistanceState(NewRaftPersistanceStateParams{
				CurrentTerm: 6,
				VotedFor:    1,
				Logs: []common.Log{
					{Term: 6, Command: "set y 10", ClientID: 6, SequenceNum: 1},
					{Term: 6, Command: "set y 11", ClientID: 6, SequenceNum: 2},
				},
			}),
			wantLastLogIndex: 10,
			wantErr:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &tt.raft
			gotLastLogIndex, err := r.Deserialize(tt.args.data, tt.args.latestSnapshot)
			if (err != nil) != tt.wantErr {
				t.Errorf("RaftPersistanceState.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotLastLogIndex != tt.wantLastLogIndex {
				t.Errorf("RaftPersistanceState.Deserialize() = %v, want %v", gotLastLogIndex, tt.wantLastLogIndex)
			}
			if !reflect.DeepEqual(tt.raft, tt.want) {
				t.Errorf("RaftPersistanceState.Deserialize() = %v, want %v", tt.raft, tt.want)
			}
		})
	}
}

func TestRaftPersistanceStateImpl_GetLog(t *testing.T) {
	type fields struct {
		logs           []common.Log
		latestSnapshot common.SnapshotMetadata
	}
	type args struct {
		index int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    common.Log
		wantErr error
	}{
		{
			name: "empty log",
			fields: fields{
				logs:           []common.Log{},
				latestSnapshot: common.SnapshotMetadata{},
			},
			args: args{
				index: 0,
			},
			want:    common.Log{},
			wantErr: common.ErrLogIsEmpty,
		},
		{
			name: "index out of range",
			fields: fields{
				logs:           []common.Log{{Term: 1, Command: "set x 1"}},
				latestSnapshot: common.SnapshotMetadata{},
			},
			args: args{
				index: 0,
			},
			want:    common.Log{},
			wantErr: common.ErrIndexOutOfRange,
		},
		{
			name: "index out of range",
			fields: fields{
				logs:           []common.Log{{Term: 1, Command: "set x 1"}},
				latestSnapshot: common.SnapshotMetadata{},
			},
			args: args{
				index: 2,
			},
			want:    common.Log{},
			wantErr: common.ErrIndexOutOfRange,
		},
		{
			name: "index is in range",
			fields: fields{
				logs:           []common.Log{{Term: 1, Command: "set x 1"}},
				latestSnapshot: common.SnapshotMetadata{},
			},
			args: args{
				index: 1,
			},
			want:    common.Log{Term: 1, Command: "set x 1"},
			wantErr: nil,
		},
		{
			name: "log is in snapshot",
			fields: fields{
				logs:           []common.Log{{Term: 4, Command: "set x 4"}},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 3, LastLogIndex: 3},
			},
			args: args{
				index: 1,
			},
			want:    common.Log{Term: 3},
			wantErr: common.ErrLogIsInSnapshot,
		},
		{
			name: "log is not in snapshot",
			fields: fields{
				logs:           []common.Log{{Term: 4, Command: "set x 4"}},
				latestSnapshot: common.SnapshotMetadata{LastLogTerm: 3, LastLogIndex: 3},
			},
			args: args{
				index: 4,
			},
			want:    common.Log{Term: 4, Command: "set x 4"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := RaftPersistanceStateImpl{
				logs:           tt.fields.logs,
				latestSnapshot: tt.fields.latestSnapshot,
			}
			got, err := r.GetLog(tt.args.index)
			if (err != nil) && !errors.Is(err, tt.wantErr) {
				t.Errorf("RaftPersistanceStateImpl.GetLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RaftPersistanceStateImpl.GetLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRaftPersistanceStateImpl_LastLogInfo(t *testing.T) {
	type fields struct {
		votedFor       int
		currentTerm    int
		logs           []common.Log
		latestSnapshot common.SnapshotMetadata
		storage        StorageInterface
	}
	tests := []struct {
		name      string
		fields    fields
		wantIndex int
		wantTerm  int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftPersistanceStateImpl{
				votedFor:       tt.fields.votedFor,
				currentTerm:    tt.fields.currentTerm,
				logs:           tt.fields.logs,
				latestSnapshot: tt.fields.latestSnapshot,
				storage:        tt.fields.storage,
			}
			gotIndex, gotTerm := n.LastLogInfo()
			if gotIndex != tt.wantIndex {
				t.Errorf("RaftPersistanceStateImpl.LastLogInfo() gotIndex = %v, want %v", gotIndex, tt.wantIndex)
			}
			if gotTerm != tt.wantTerm {
				t.Errorf("RaftPersistanceStateImpl.LastLogInfo() gotTerm = %v, want %v", gotTerm, tt.wantTerm)
			}
		})
	}
}
