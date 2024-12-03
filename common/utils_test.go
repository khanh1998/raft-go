package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandInt(t *testing.T) {
	assert.Panics(t, func() {
		RandInt(0, 0)
	})
	r := RandInt(0, 1)
	assert.Equal(t, int64(0), r)
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
