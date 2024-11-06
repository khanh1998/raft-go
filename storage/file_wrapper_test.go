package storage

import (
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileWrapper_GetFileNames(t *testing.T) {
	tests := []struct {
		name      string
		setup     func() (string, error)
		wantNames []string
		wantErr   bool
	}{
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				for _, name := range []string{"wal.001.dat", "snapshot.001.dat"} {
					path := dir + "/" + name
					_, err := os.Create(path)
					if err != nil {
						return dir, err
					}
				}
				return dir, nil
			},
			wantNames: []string{"wal.001.dat", "snapshot.001.dat"},
			wantErr:   false,
		},
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				return dir + "/non-exist", nil // make the returned dir non-exist
			},
			wantNames: nil,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := tt.setup()
			assert.NoError(t, err)

			f := FileWrapperImpl{}
			gotNames, err := f.GetFileNames(dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapper.GetFileNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			sort.Strings(gotNames)
			sort.Strings(tt.wantNames)

			if !reflect.DeepEqual(gotNames, tt.wantNames) {
				t.Errorf("FileWrapper.GetFileNames() = %v, want %v", gotNames, tt.wantNames)
			}
		})
	}
}

func TestFileWrapper_ReadStrings(t *testing.T) {
	type args struct {
		path string
	}

	tests := []struct {
		setup   func() (string, error)
		name    string
		args    args
		want    []string
		want1   int64
		wantErr bool
	}{
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:    args{path: "wal.001.dat"},
			want:    []string{"name=khanh", "age=26", "nation=vietnam"},
			want1:   33,
			wantErr: false,
		},
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:    args{path: "wal.002.dat"},
			want:    nil,
			want1:   0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		dir, err := tt.setup()
		assert.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperImpl{}
			got, got1, err := f.ReadStrings(dir + "/" + tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapper.ReadStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			sort.Strings(got)
			sort.Strings(tt.want)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileWrapper.ReadStrings() got = %v, want %v", got, tt.want)
			}

			if got1 != tt.want1 {
				t.Errorf("FileWrapper.ReadStrings() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestFileWrapper_AppendStrings(t *testing.T) {
	type args struct {
		path  string
		lines []string
	}
	tests := []struct {
		name         string
		setup        func() (string, error)
		args         args
		wantFileSize int64
		wantErr      bool
	}{
		{
			name: "write to new file",
			setup: func() (string, error) {
				dir := t.TempDir()
				return dir, nil
			},
			args:         args{path: "wal.001.dat", lines: []string{"name=khanh", "age=26", "nation=vietnam"}},
			wantFileSize: 33,
			wantErr:      false,
		},
		{
			name: "empty input data",
			setup: func() (string, error) {
				dir := t.TempDir()
				return dir, nil
			},
			args:         args{path: "wal.001.dat", lines: nil},
			wantFileSize: 0,
			wantErr:      true,
		},
		{
			name: "append to a current file",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:         args{path: "wal.001.dat", lines: []string{"name=khanh", "age=26", "nation=vietnam"}},
			wantFileSize: 66,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := tt.setup()
			assert.NoError(t, err)

			f := FileWrapperImpl{}
			gotFileSize, err := f.AppendStrings(dir+"/"+tt.args.path, tt.args.lines)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapper.AppendStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFileSize != tt.wantFileSize {
				t.Errorf("FileWrapper.AppendStrings() = %v, want %v", gotFileSize, tt.wantFileSize)
			}
		})
	}
}

func TestFileWrapper_AppendKeyValuePairs(t *testing.T) {
	type args struct {
		path      string
		keyValues []string
	}
	tests := []struct {
		name    string
		setup   func() (string, error)
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "write to new file",
			setup: func() (string, error) {
				dir := t.TempDir()
				return dir, nil
			},
			args:    args{path: "wal.001.dat", keyValues: []string{"name", "khanh", "age", "26", "nation", "vietnam"}},
			want:    33,
			wantErr: false,
		},
		{
			name: "empty input data",
			setup: func() (string, error) {
				dir := t.TempDir()
				return dir, nil
			},
			args:    args{path: "wal.001.dat", keyValues: nil},
			want:    0,
			wantErr: true,
		},
		{
			name: "append to a current file",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:    args{path: "wal.001.dat", keyValues: []string{"name", "khanh", "age", "26", "nation", "vietnam"}},
			want:    66,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := tt.setup()
			assert.NoError(t, err)

			f := FileWrapperImpl{}
			got, err := f.AppendKeyValuePairs(dir+"/"+tt.args.path, tt.args.keyValues...)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapper.AppendKeyValuePairs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FileWrapper.AppendKeyValuePairs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileWrapper_ReadKeyValuePairsToArray(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		setup   func() (string, error)
		args    args
		want    []string
		want1   int64
		wantErr bool
	}{
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:    args{path: "wal.001.dat"},
			want:    []string{"name", "khanh", "age", "26", "nation", "vietnam"},
			want1:   33,
			wantErr: false,
		},
		{
			name: "",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "wal.001.dat"
				content := []byte("name=khanh\nage=26\nnation=vietnam\n")
				return dir, os.WriteFile(path, content, 0666)
			},
			args:    args{path: "wal.002.dat"},
			want:    nil,
			want1:   0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := tt.setup()
			assert.NoError(t, err)

			f := FileWrapperImpl{}
			got, got1, err := f.ReadKeyValuePairsToArray(dir + "/" + tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapper.ReadKeyValuePairsToArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileWrapper.ReadKeyValuePairsToArray() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("FileWrapper.ReadKeyValuePairsToArray() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestFileWrapper_DeleteFile(t *testing.T) {
	dir := t.TempDir()
	f := FileWrapperImpl{}
	fileName := dir + "/" + "wal.001.txt"
	_, err := os.Create(fileName)
	assert.NoError(t, err)
	err = f.DeleteFile(fileName)
	assert.NoError(t, err)

	// delete non exist file
	fileName = dir + "/" + "wal.002.txt"
	err = f.DeleteFile(fileName)
	assert.Error(t, err)
}

func TestFileWrapper_Rename(t *testing.T) {
	dir := t.TempDir()
	f := FileWrapperImpl{}
	fileName := dir + "/" + "wal.001.txt"
	fileName1 := dir + "/" + "wal.002.txt"
	_, err := os.Create(fileName)
	assert.NoError(t, err)
	err = f.Rename(fileName, fileName1)
	assert.NoError(t, err)

	// rename non exist file
	fileName2 := dir + "/" + "wal.003.txt"
	fileName3 := dir + "/" + "wal.004.txt"
	err = f.Rename(fileName2, fileName3)
	assert.Error(t, err)
}
