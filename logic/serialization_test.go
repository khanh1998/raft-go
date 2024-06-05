package logic

import (
	"khanh/raft-go/common"
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
		want   map[string]string
	}{
		{
			fields: fields{
				CurrentTerm: 3,
				VotedFor:    0,
				Logs:        []common.Log{},
			},
			want: map[string]string{
				"current_term": "3",
				"voted_for":    "0",
				"log_count":    "0",
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
			want: map[string]string{
				"current_term": "3",
				"voted_for":    "1",
				"log_count":    "2",
				"log_0":        "1|set x 1|0|0",
				"log_1":        "2|set y 3|0|0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := RaftBrainImpl{
				currentTerm: tt.fields.CurrentTerm,
				votedFor:    tt.fields.VotedFor,
				logs:        tt.fields.Logs,
			}
			if got := n.serialize(false, false, ""); !reflect.DeepEqual(got, tt.want) {
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
		data map[string]string
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
				data: map[string]string{
					"current_term": "1",
					"voted_for":    "3",
					"log_count":    "0",
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
				data: map[string]string{
					"current_term": "1",
					"voted_for":    "3",
					"log_count":    "2",
					"log_0":        "1|set x 1|3|2",
					"log_1":        "2|set y 3|5|1",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{}
			if err := n.deserialize(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("nodeImpl.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if n.currentTerm != tt.want.CurrentTerm {
				t.Errorf("current term val = %v, want wal %v", n.currentTerm, tt.want.CurrentTerm)
			}

			if n.votedFor != tt.want.VotedFor {
				t.Errorf("current term val = %v, want wal %v", n.votedFor, tt.want.VotedFor)
			}

			if !reflect.DeepEqual(n.serialize(false, false, ""), tt.args.data) {
				t.Errorf("current term val = %v, want wal %v", n.serialize(false, false, ""), tt.args.data)
			}

			if !reflect.DeepEqual(n.logs, tt.want.Logs) {
				t.Errorf("current term val = %v, want wal %v", n.logs, tt.want.Logs)
			}
		})
	}
}
