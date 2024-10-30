package common

import (
	"reflect"
	"testing"
)

func TestLog_ToString(t *testing.T) {
	type fields struct {
		Term         int
		Command      string
		ClientID     int
		SequenceNum  int
		ClusterClock uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "",
			fields: fields{
				Term:         1,
				Command:      "set x 100",
				ClientID:     5,
				SequenceNum:  7,
				ClusterClock: 0,
			},
			want: "1|5|7|0|set x 100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := Log{
				Term:        tt.fields.Term,
				Command:     tt.fields.Command,
				ClientID:    tt.fields.ClientID,
				SequenceNum: tt.fields.SequenceNum,
			}
			if got := l.ToString(); got != tt.want {
				t.Errorf("Log.ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewLogFromString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    Log
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				s: "5|9|10|1|set x 250",
			},
			want: Log{
				Term:        5,
				Command:     "set x 250",
				ClientID:    9,
				SequenceNum: 10,
				ClusterTime: 1,
			},
			wantErr: false,
		},
		{
			name: "not ok",
			args: args{
				s: "hehe|set x 250",
			},
			want: Log{
				Term:        0,
				Command:     "",
				ClientID:    0,
				SequenceNum: 0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLogFromString(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLogFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}
