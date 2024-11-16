package storage

import (
	"reflect"
	"sort"
	"testing"
)

func TestFileWrapperMock_ReadStrings(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		want1   int64
		wantErr bool
	}{
		{
			name: "file exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"set counter 10", "set counter 11"},
				},
				size: map[string]int64{
					"wal.dat": 28,
				},
			},
			args: args{
				path: "wal.dat",
			},
			want:    []string{"set counter 10", "set counter 11"},
			want1:   28,
			wantErr: false,
		},
		{
			name: "file doesn't exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"set counter 10", "set counter 11"},
				},
				size: map[string]int64{
					"wal.dat": 28,
				},
			},
			args: args{
				path: "wal001.dat",
			},
			want:    nil,
			want1:   0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			got, got1, err := f.ReadStrings(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.ReadStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileWrapperMock.ReadStrings() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("FileWrapperMock.ReadStrings() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestFileWrapperMock_AppendStrings(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		path  string
		lines []string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantFileSize int64
		wantData     []string
		wantErr      bool
	}{
		{
			name: "file exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"set counter 10", "set counter 11"},
				},
				size: map[string]int64{
					"wal.dat": 28,
				},
			},
			args: args{
				path:  "wal.dat",
				lines: []string{"set counter 12", "set counter 13"},
			},
			wantFileSize: 56,
			wantData:     []string{"set counter 10", "set counter 11", "set counter 12", "set counter 13"},
			wantErr:      false,
		},
		{
			name: "file doesn't exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"set counter 10", "set counter 11"},
				},
				size: map[string]int64{
					"wal.dat": 28,
				},
			},
			args: args{
				path:  "wal001.dat",
				lines: []string{"set counter 12", "set counter 13"},
			},
			wantFileSize: 28,
			wantData:     []string{"set counter 12", "set counter 13"},
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			gotFileSize, err := f.AppendStrings(tt.args.path, tt.args.lines)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.AppendStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFileSize != tt.wantFileSize {
				t.Errorf("FileWrapperMock.AppendStrings() = %v, want %v", gotFileSize, tt.wantFileSize)
			}

			if !reflect.DeepEqual(tt.wantData, f.Data[tt.args.path]) {
				t.Errorf("FileWrapperMock.AppendStrings() = %v, want %v", f.Data[tt.args.path], tt.wantData)
			}
		})
	}
}

func TestFileWrapperMock_AppendKeyValuePairs(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		path      string
		keyValues []string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     int64
		wantData []string
		wantErr  bool
	}{
		{
			name: "file exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"counter=10", "counter=11"},
				},
				size: map[string]int64{
					"wal.dat": 20,
				},
			},
			args: args{
				path:      "wal.dat",
				keyValues: []string{"counter", "12", "counter", "13"},
			},
			want:     40,
			wantData: []string{"counter=10", "counter=11", "counter=12", "counter=13"},
			wantErr:  false,
		},
		{
			name: "file doesn't exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"counter=10", "counter=11"},
				},
				size: map[string]int64{
					"wal.dat": 20,
				},
			},
			args: args{
				path:      "wal001.dat",
				keyValues: []string{"counter", "12", "counter", "13"},
			},
			want:     20,
			wantData: []string{"counter=12", "counter=13"},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			got, err := f.AppendKeyValuePairs(tt.args.path, tt.args.keyValues...)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.AppendKeyValuePairs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FileWrapperMock.AppendKeyValuePairs() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.wantData, f.Data[tt.args.path]) {
				t.Errorf("FileWrapperMock.AppendKeyValuePairs() = %v, want %v", f.Data[tt.args.path], tt.wantData)
			}
		})
	}
}

func TestFileWrapperMock_ReadKeyValuePairsToArray(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		want1   int64
		wantErr bool
	}{
		{
			name: "file exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"counter=10", "counter=11"},
				},
				size: map[string]int64{
					"wal.dat": 20,
				},
			},
			args: args{
				path: "wal.dat",
			},
			want1:   20,
			want:    []string{"counter", "10", "counter", "11"},
			wantErr: false,
		},
		{
			name: "file doesn't exist",
			fields: fields{
				data: map[string][]string{
					"wal.dat": {"counter=10", "counter=11"},
				},
				size: map[string]int64{
					"wal.dat": 20,
				},
			},
			args: args{
				path: "wal001.dat",
			},
			want1:   0,
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			got, got1, err := f.ReadKeyValuePairsToArray(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.ReadKeyValuePairsToArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileWrapperMock.ReadKeyValuePairsToArray() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("FileWrapperMock.ReadKeyValuePairsToArray() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestFileWrapperMock_GetFileNames(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		folder string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantNames []string
		wantErr   bool
	}{
		{
			name: "file exist",
			fields: fields{
				data: map[string][]string{
					"data/wal001.dat":  {"counter=10", "counter=11"},
					"data/wal002.dat":  {"counter=12", "counter=13"},
					"data/wal003.dat":  {"counter=14", "counter=15"},
					"data/snap001.dat": {"snapshot=001"},
					"data/snap002.dat": {"snapshot=002"},
					"data/snap003.dat": {"snapshot=003"},
					"tmp/tmp.dat":      {"something"},
				},
				size: map[string]int64{},
			},
			args: args{
				folder: "data/",
			},
			wantNames: []string{"wal001.dat", "wal002.dat", "wal003.dat", "snap001.dat", "snap002.dat", "snap003.dat"},
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			gotNames, err := f.GetFileNames(tt.args.folder)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.GetFileNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			sort.Strings(tt.wantNames)
			sort.Strings(gotNames)

			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("FileWrapperMock.GetFileNames() = %v, want %v", gotNames, tt.wantNames)
			}
		})
	}
}

func TestFileWrapperMock_Rename(t *testing.T) {
	type fields struct {
		data map[string][]string
		size map[string]int64
	}
	type args struct {
		oldPath string
		newPath string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    fields
	}{
		{
			name: "",
			fields: fields{
				data: map[string][]string{
					"data/wal.001.dat": {"data"},
				},
				size: map[string]int64{
					"data/wal.001.dat": 4,
				},
			},
			args: args{
				oldPath: "data/wal.001.dat",
				newPath: "data/wal.002.dat",
			},
			wantErr: false,
			want: fields{
				data: map[string][]string{
					"data/wal.002.dat": {"data"},
				},
				size: map[string]int64{
					"data/wal.002.dat": 4,
				},
			},
		},
		{
			name: "",
			fields: fields{
				data: map[string][]string{
					"data/wal.001.dat": {"data"},
				},
				size: map[string]int64{
					"data/wal.001.dat": 4,
				},
			},
			args: args{
				oldPath: "data/wal.003.dat",
				newPath: "data/wal.002.dat",
			},
			wantErr: true,
			want: fields{
				data: map[string][]string{
					"data/wal.001.dat": {"data"},
				},
				size: map[string]int64{
					"data/wal.001.dat": 4,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.data,
				Size: tt.fields.size,
			}
			if err := f.Rename(tt.args.oldPath, tt.args.newPath); (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.Rename() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(f.Data, tt.want.data) {
				t.Errorf("FileWrapperMock.Rename() data = %v, want %v", f.Data, tt.want.data)
			}
			if !reflect.DeepEqual(f.Size, tt.want.size) {
				t.Errorf("FileWrapperMock.Rename() size = %v, want %v", f.Size, tt.want.size)
			}

		})
	}
}

func TestFileWrapperMock_ReadFirstOccurrenceKeyValuePairsToArray(t *testing.T) {
	type fields struct {
		Data map[string][]string
		Size map[string]int64
	}
	type args struct {
		path string
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				Data: map[string][]string{
					"data/wal.0001.dat": {"name=khanh", "age=26", "city=hcm", "name=felix"},
				},
				Size: map[string]int64{
					"data/wal.0001.dat": 30,
				},
			},
			args: args{
				path: "data/wal.0001.dat",
				keys: []string{"name", "nation"},
			},
			want:    []string{"name", "khanh"},
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				Data: map[string][]string{
					"data/wal.0001.dat": {"name=khanh", "age=26", "city=hcm", "name=felix"},
				},
				Size: map[string]int64{
					"data/wal.0001.dat": 30,
				},
			},
			args: args{
				path: "data/wal.0002.dat",
				keys: []string{"name", "nation"},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperMock{
				Data: tt.fields.Data,
				Size: tt.fields.Size,
			}
			got, err := f.ReadFirstOccurrenceKeyValuePairsToArray(tt.args.path, tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperMock.ReadFirstOccurrenceKeyValuePairsToArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileWrapperMock.ReadFirstOccurrenceKeyValuePairsToArray() = %v, want %v", got, tt.want)
			}
		})
	}
}
