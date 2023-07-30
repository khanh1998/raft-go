package common

import "testing"

func TestKeyValueStateMachine_Process(t *testing.T) {
	type fields struct {
		data map[string]string
	}
	type args struct {
		command string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantResult string
		wantErr    bool
	}{
		{
			name: "get ok",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
			},
			args: args{
				command: "get name",
			},
			wantResult: "khanh",
			wantErr:    false,
		},
		{
			name: "get not ok",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
			},
			args: args{
				command: "get age",
			},
			wantResult: "",
			wantErr:    true,
		},
		{
			name: "set ok",
			fields: fields{
				data: map[string]string{
					"name": "khanh",
				},
			},
			args: args{
				command: "set age 25",
			},
			wantResult: "25",
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KeyValueStateMachine{
				data: tt.fields.data,
			}
			gotResult, err := k.Process(tt.args.command)
			if (err != nil) != tt.wantErr {
				t.Errorf("KeyValueStateMachine.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotResult != tt.wantResult {
				t.Errorf("KeyValueStateMachine.Process() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}
