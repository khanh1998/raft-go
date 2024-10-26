package logic

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"reflect"
	"testing"
)

func Test_nodeImpl_Serialize(t *testing.T) {
	type fields struct {
		CurrentTerm int
		VotedFor    int
		Logs        []common.Log
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			fields: fields{
				CurrentTerm: 3,
				VotedFor:    0,
				Logs:        []common.Log{},
			},
			want: []string{
				"current_term", "3",
				"voted_for", "0",
			},
		},
		{
			fields: fields{
				CurrentTerm: 3,
				VotedFor:    1,
				Logs: []common.Log{
					{Term: 1, Command: "set x 1"},
					{Term: 2, Command: "set y 3"},
				},
			},
			want: []string{
				"current_term", "3",
				"voted_for", "1",
				"append_log", common.Log{Term: 1, Command: "set x 1"}.ToString(),
				"append_log", common.Log{Term: 2, Command: "set y 3"}.ToString(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := RaftBrainImpl{
				currentTerm: tt.fields.CurrentTerm,
				votedFor:    tt.fields.VotedFor,
				logs:        tt.fields.Logs,
				logger:      observability.NewOtelLogger(),
			}

			if got := n.serializeToArray(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nodeImpl.Serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nodeImpl_Deserialize(t *testing.T) {
	type fields struct {
		CurrentTerm int
		VotedFor    int
		Logs        []common.Log
	}
	type args struct {
		data []string
	}
	tests := []struct {
		name    string
		want    fields
		args    args
		wantErr bool
	}{
		{
			want: fields{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs:        []common.Log{},
			},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
				},
			},
			wantErr: false,
		},
		{
			want: fields{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs: []common.Log{
					{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2},
					{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1},
				},
			},
			args: args{
				data: []string{
					"current_term", "1",
					"voted_for", "3",
					"append_log", common.Log{Term: 1, Command: "set x 1", ClientID: 3, SequenceNum: 2}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 3", ClientID: 5, SequenceNum: 1}.ToString(),
					"append_log", common.Log{Term: 2, Command: "set y 4", ClientID: 5, SequenceNum: 1}.ToString(),
					"delete_log", "1",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{}
			if err := n.deserializeFromArray(context.Background(), tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("nodeImpl.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if n.currentTerm != tt.want.CurrentTerm {
				t.Errorf("current term = %v, want %v", n.currentTerm, tt.want.CurrentTerm)
			}

			if n.votedFor != tt.want.VotedFor {
				t.Errorf("voted for = %v, want %v", n.votedFor, tt.want.VotedFor)
			}

			// if !reflect.DeepEqual(n.serializeToArray(), tt.args.data) {
			// 	t.Errorf("n.serializeToArray() = %v, want %v", n.serializeToArray(), tt.args.data)
			// }

			if !reflect.DeepEqual(n.logs, tt.want.Logs) {
				t.Errorf("logs = %v, want %v", n.logs, tt.want.Logs)
			}
		})
	}
}
