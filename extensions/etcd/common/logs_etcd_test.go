package common

import (
	gc "khanh/raft-go/common"
	"reflect"
	"testing"
)

func TestEtcdCommand_Causes(t *testing.T) {
	type fields struct {
		PrevExist *bool
		PrevValue *string
		PrevIndex int
	}
	tests := []struct {
		name     string
		fields   fields
		wantData string
	}{
		{
			name: "1",
			fields: fields{
				PrevExist: gc.GetPointer(true),
				PrevValue: gc.GetPointer("abc"),
				PrevIndex: 4,
			},
			wantData: "[prevExist=true,prevValue=abc,prevIndex=4]",
		},
		{
			name: "2",
			fields: fields{
				PrevExist: gc.GetPointer(false),
				PrevValue: gc.GetPointer(""),
				PrevIndex: 0,
			},
			wantData: "[prevExist=false,prevValue=]",
		},
		{
			name: "3",
			fields: fields{
				PrevExist: gc.GetPointer(false),
				PrevValue: nil,
				PrevIndex: 0,
			},
			wantData: "[prevExist=false]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := EtcdCommand{
				PrevExist: tt.fields.PrevExist,
				PrevValue: tt.fields.PrevValue,
				PrevIndex: tt.fields.PrevIndex,
			}
			if gotData := e.Causes(); !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("EtcdCommand.Causes() = %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func TestEtcdLog_ToString(t *testing.T) {
	type fields struct {
		Term    int
		Command EtcdCommand
		Time    uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "",
			fields: fields{
				Term: 1,
				Command: EtcdCommand{
					Action: "set",
					Key:    "name",
					Value:  gc.GetPointer("khanh"),
					Ttl:    123,
				},
				Time: 12345,
			},
			want: `{"term":1,"command":{"action":"set","key":"name","value":"khanh","ttl":123},"time":12345}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := EtcdLog{
				Term:    tt.fields.Term,
				Command: tt.fields.Command,
				Time:    tt.fields.Time,
			}
			if got := e.ToString(); got != tt.want {
				t.Errorf("EtcdLog.ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdLogFactory_FromString(t *testing.T) {
	type args struct {
		data string
	}
	tests := []struct {
		name    string
		args    args
		want    gc.Log
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				data: EtcdLog{Term: 1, Time: 1234, Command: EtcdCommand{
					Action: "get", Wait: true, WaitIndex: 123,
				}}.ToString(),
			},
			want: EtcdLog{Term: 1, Time: 1234, Command: EtcdCommand{
				Action: "get", Wait: true, WaitIndex: 123,
			}},
			wantErr: false,
		},
		{
			name: "empty",
			args: args{
				data: "{}",
			},
			want:    EtcdLog{},
			wantErr: false,
		},
		{
			name: "error",
			args: args{
				data: "this is data",
			},
			want:    EtcdLog{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := EtcdLogFactory{}
			got, err := c.FromString(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("EtcdLogFactory.FromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EtcdLogFactory.FromString() = %v, want %v", got, tt.want)
			}
		})
	}
}
