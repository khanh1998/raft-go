package common

import (
	"reflect"
	"testing"
)

func TestClientEntry_FromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name         string
		args         args
		want         ClientEntry
		wantClientId int
		wantErr      bool
	}{
		{
			name: "1",
			args: args{
				str: "1|2|100|abc",
			},
			wantClientId: 1,
			want: ClientEntry{
				LastSequenceNum: 2,
				ExpiryTime:      100,
				LastResponse:    "abc",
			},
			wantErr: false,
		},
		{
			name: "2",
			args: args{
				str: "1|2|100|",
			},
			wantClientId: 1,
			want: ClientEntry{
				LastSequenceNum: 2,
				ExpiryTime:      100,
				LastResponse:    "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientEntry{}
			gotClientId, err := c.FromString(tt.args.str)
			if (err != nil) != tt.wantErr {
				t.Errorf("ClientEntry.FromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotClientId != tt.wantClientId {
				t.Errorf("ClientEntry.FromString() = %v, want %v", gotClientId, tt.wantClientId)
			}

			if !reflect.DeepEqual(*c, tt.want) {
				t.Errorf("ClientEntry.FromString() = %v, want %v", *c, tt.want)
			}
		})
	}
}
