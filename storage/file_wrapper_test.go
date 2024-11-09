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

func TestFileWrapperImpl_ReadAt(t *testing.T) {
	data := `last_log_index=6
last_log_term=6
member_count=0
session_count=1
key_value_count=4
key_lock_count=2
2|5|143483080957|1
city=hcm
counter=1
name=khanh
gender=male
name=2
city=2
`
	fileName := "text.dat"
	setup := func() (string, error) {
		dir := t.TempDir()
		path := dir + "/" + fileName
		data := []byte(data)
		err := os.WriteFile(path, data, 0644)
		if err != nil {
			return "", err
		}
		return dir, nil
	}
	type args struct {
		path      string
		offset    int64
		maxLength int
	}
	tests := []struct {
		name     string
		args     args
		wantData []byte
		wantEof  bool
		wantErr  bool
	}{
		{
			name: "read at beginning",
			args: args{
				path:      fileName,
				offset:    0,
				maxLength: 20,
			},
			wantData: []byte("last_log_index=6\nlas"),
			wantEof:  false,
			wantErr:  false,
		},
		{
			name: "read at middle",
			args: args{
				path:      fileName,
				offset:    20,
				maxLength: 20,
			},
			wantData: []byte("t_log_term=6\nmember_"),
			wantEof:  false,
			wantErr:  false,
		},
		{
			name: "read to the end",
			args: args{
				path:      fileName,
				offset:    159,
				maxLength: 20,
			},
			wantData: []byte("\nname=2\ncity=2\n"),
			wantEof:  true,
			wantErr:  false,
		},
		{
			name: "read before the end",
			args: args{
				path:      fileName,
				offset:    int64(len(data) - 1),
				maxLength: 20,
			},
			wantData: []byte("\n"),
			wantEof:  true,
			wantErr:  false,
		},
		{
			name: "read at the end",
			args: args{
				path:      fileName,
				offset:    int64(len(data)),
				maxLength: 20,
			},
			wantData: []byte(""),
			wantEof:  true,
			wantErr:  false,
		},
		{
			name: "read after the end",
			args: args{
				path:      fileName,
				offset:    int64(len(data) + 1),
				maxLength: 20,
			},
			wantData: []byte(""),
			wantEof:  true,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperImpl{}

			dir, err := setup()
			assert.NoError(t, err)

			path := dir + "/" + tt.args.path
			gotData, gotEof, err := f.ReadAt(path, tt.args.offset, tt.args.maxLength)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperImpl.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("FileWrapperImpl.ReadAt() gotData = %s, want %s", gotData, tt.wantData)
				t.Errorf("FileWrapperImpl.ReadAt() gotData = %v, want %v", gotData, tt.wantData)
			}
			if gotEof != tt.wantEof {
				t.Errorf("FileWrapperImpl.ReadAt() gotEof = %v, want %v", gotEof, tt.wantEof)
			}
		})
	}
}

func TestFileWrapperImpl_WriteAt(t *testing.T) {
	type args struct {
		path   string
		offset int64
		data   []byte
	}
	tests := []struct {
		name        string
		setup       func() (string, error)
		args        args
		wantSize    int
		wantErr     bool
		wantContent []byte
	}{
		{
			name: "write to non-exist file",
			setup: func() (string, error) {
				return t.TempDir(), nil
			},
			args: args{
				path:   "text.dat",
				offset: 0,
				data:   []byte("name=khanh\ngender=male\nnation=vietnam\n"),
			},
			wantSize:    38,
			wantErr:     false,
			wantContent: []byte("name=khanh\ngender=male\nnation=vietnam\n"),
		},
		{
			name: "write to non-exist file, offset > 0",
			setup: func() (string, error) {
				return t.TempDir(), nil
			},
			args: args{
				path:   "text.dat",
				offset: 10,
				data:   []byte("name=khanh\ngender=male\nnation=vietnam\n"),
			},
			wantSize:    38,
			wantErr:     false,
			wantContent: append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []byte("name=khanh\ngender=male\nnation=vietnam\n")...),
		},
		{
			name: "write to exist file, offset > 0",
			setup: func() (string, error) {
				dir := t.TempDir()
				path := dir + "/" + "text.dat"
				emptyData := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
				data := append(emptyData, []byte("name=khanh\ngender=male\nnation=vietnam\n")...)
				os.WriteFile(path, data, 0644)
				return dir, nil
			},
			args: args{
				path:   "text.dat",
				offset: 0,
				data:   []byte("10bytes__\n"),
			},
			wantSize:    10,
			wantErr:     false,
			wantContent: []byte("10bytes__\nname=khanh\ngender=male\nnation=vietnam\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FileWrapperImpl{}
			dir, err := tt.setup()
			assert.NoError(t, err)

			path := dir + "/" + tt.args.path
			gotSize, err := f.WriteAt(path, tt.args.offset, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileWrapperImpl.WriteAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotSize != tt.wantSize {
				t.Errorf("FileWrapperImpl.WriteAt() size = %v, want %v", gotSize, tt.wantSize)
			}

			content, err := os.ReadFile(path)
			assert.NoError(t, err)
			if !reflect.DeepEqual(content, tt.wantContent) {
				t.Errorf("FileWrapperImpl.WriteAt() content = %v, want %v", content, tt.wantContent)
				t.Errorf("FileWrapperImpl.WriteAt() content = %s, want %s", content, tt.wantContent)
			}
		})
	}
}
