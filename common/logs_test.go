package common

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntry_ToString(t *testing.T) {
	e := Entry{Key: "x", Value: 1}
	assert.Equal(t, "x,1,", e.ToString())
	e = Entry{Key: "x", Value: 100, Opcode: Divide}
	assert.Equal(t, "x,100,div", e.ToString())
}

func TestEntry_NewEntryFromString(t *testing.T) {
	entry, err := NewEntryFromString("y,2058,")
	assert.NoError(t, err)
	assert.Equal(t, Entry{Key: "y", Value: 2058}, entry)

	entry, err = NewEntryFromString("y,03,mul")
	assert.NoError(t, err)
	assert.Equal(t, Entry{Key: "y", Value: 3, Opcode: Multiply}, entry)
}

func TestLog_ToString(t *testing.T) {
	type fields struct {
		Term   int
		Values []Entry
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{1, []Entry{{Key: "x", Value: 123}}},
			want:   "1|x,123,;",
		},
		{
			fields: fields{3, []Entry{{Key: "x", Value: 123}, {Key: "y", Value: 456}}},
			want:   "3|x,123,;y,456,;",
		},
		{
			fields: fields{5, []Entry{{"x", 123, Multiply}, {"y", 456, Divide}}},
			want:   "5|x,123,mul;y,456,div;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := Log{
				Term:   tt.fields.Term,
				Values: tt.fields.Values,
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
			args:    args{"1|"},
			want:    Log{1, []Entry{}},
			wantErr: false,
		},
		{
			args:    args{"1|x,123,;"},
			want:    Log{1, []Entry{{Key: "x", Value: 123}}},
			wantErr: false,
		},
		{
			args:    args{"1|x,123,;y,456,"},
			want:    Log{1, []Entry{{Key: "x", Value: 123}, {Key: "y", Value: 456}}},
			wantErr: false,
		},
		{
			args:    args{"1|x,123,div;y,456,overwrite"},
			want:    Log{1, []Entry{{"x", 123, Divide}, {"y", 456, Overwrite}}},
			wantErr: false,
		},
		{
			args:    args{"1|x,123,div;y,456,fake"},
			want:    Log{},
			wantErr: true,
		},
		{
			args:    args{"1|x,123,div;y,456,fake|"},
			want:    Log{},
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
