package common

import (
	"reflect"
	"testing"

	gc "khanh/raft-go/common"
)

func TestSnapshotMetadata_FromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    gc.SnapshotMetadata
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				str: "snapshot.1001.dat|5|6",
			},
			want: gc.SnapshotMetadata{
				LastLogIndex: 5,
				LastLogTerm:  6,
				FileName:     "snapshot.1001.dat",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &gc.SnapshotMetadata{}
			if err := s.FromString(tt.args.str); (err != nil) != tt.wantErr {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(*s, tt.want) {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", *s, tt.want)
			}
		})
	}
}
