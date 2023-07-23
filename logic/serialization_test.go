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
				Logs:        []common.Log{{1, []common.Entry{{"x", 1, common.Divide}}}, {2, []common.Entry{{"x", 1, common.Multiply}, {"y", 3, common.Plus}}}},
			},
			want: map[string]string{
				"current_term": "3",
				"voted_for":    "1",
				"log_count":    "2",
				"log_0":        "1|x,1,div;",
				"log_1":        "2|x,1,mul;y,3,plus;",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := RaftBrainImpl{
				CurrentTerm: tt.fields.CurrentTerm,
				VotedFor:    tt.fields.VotedFor,
				Logs:        tt.fields.Logs,
			}
			if got := n.Serialize(false, false, ""); !reflect.DeepEqual(got, tt.want) {
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
			args: args{map[string]string{
				"current_term": "1",
				"voted_for":    "3",
				"log_count":    "0",
			}},
			wantErr: false,
		},
		{
			want: fields{
				CurrentTerm: 1,
				VotedFor:    3,
				Logs:        []common.Log{{1, []common.Entry{{"x", 1, common.Divide}}}, {2, []common.Entry{{"x", 1, common.Multiply}, {"y", 3, common.Plus}}}}},
			args: args{map[string]string{
				"current_term": "1",
				"voted_for":    "3",
				"log_count":    "2",
				"log_0":        "1|x,1,div;",
				"log_1":        "2|x,1,mul;y,3,plus;",
			}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &RaftBrainImpl{}
			if err := n.Deserialize(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("nodeImpl.Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if n.CurrentTerm != tt.want.CurrentTerm {
				t.Errorf("current term val = %v, want wal %v", n.CurrentTerm, tt.want.CurrentTerm)
			}

			if n.VotedFor != tt.want.VotedFor {
				t.Errorf("current term val = %v, want wal %v", n.VotedFor, tt.want.VotedFor)
			}

			if !reflect.DeepEqual(n.Serialize(false, false, ""), tt.args.data) {
				t.Errorf("current term val = %v, want wal %v", n.Serialize(false, false, ""), tt.args.data)
			}

			if !reflect.DeepEqual(n.Logs, tt.want.Logs) {
				t.Errorf("current term val = %v, want wal %v", n.Logs, tt.want.Logs)
			}
		})
	}
}
