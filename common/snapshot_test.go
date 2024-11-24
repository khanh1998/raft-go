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

func TestSnapshotMetadata_FromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    SnapshotMetadata
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				str: "snapshot.1001.dat|5|6",
			},
			want: SnapshotMetadata{
				LastLogIndex: 5,
				LastLogTerm:  6,
				FileName:     "snapshot.1001.dat",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SnapshotMetadata{}
			if err := s.FromString(tt.args.str); (err != nil) != tt.wantErr {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(*s, tt.want) {
				t.Errorf("SnapshotMetadata.FromString() error = %v, wantErr %v", *s, tt.want)
			}
		})
	}
}

func TestIsSnapshotFile(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "1",
			args: args{fileName: "snapshot.1_1.dat"},
			want: true,
		},
		{
			name: "2",
			args: args{fileName: "snapshot.11.dat"},
			want: false,
		},
		{
			name: "3",
			args: args{fileName: "snapshot.a_1.dat"},
			want: false,
		},
		{
			name: "4",
			args: args{fileName: "snapshot.0001_0002.dat"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSnapshotFile(tt.args.fileName)
			if got != tt.want {
				t.Errorf("IsSnapshotFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSnapshotFileName(t *testing.T) {
	type args struct {
		term  int
		index int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{term: 0, index: 0},
			want: "snapshot.00000000000000000000_00000000000000000000.dat",
		},
		{
			name: "",
			args: args{term: 1, index: 5},
			want: "snapshot.00000000000000000001_00000000000000000005.dat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSnapshotFileName(tt.args.term, tt.args.index); got != tt.want {
				t.Errorf("NewSnapshotFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}
