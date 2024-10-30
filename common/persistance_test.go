package common

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistence_ReadKeyValuePairsToMap(t *testing.T) {
	dataFileName := "data.dat"

	keys := []string{"name", "age"}
	data := map[string]string{
		keys[0]: "khanh",
		keys[1]: "25",
	}

	p, err := NewPersistence("", dataFileName)
	assert.NoError(t, err)

	output, err := p.ReadKeyValuePairsToMap(keys)
	assert.ErrorIs(t, err, ErrEmptyData)
	assert.Equal(t, map[string]string(map[string]string(nil)), output)

	file, err := os.OpenFile(dataFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	assert.NoError(t, err)

	defer file.Close()

	writer := bufio.NewWriter(file)

	for key, value := range data {
		_, err := writer.WriteString(key + "=" + value + "\n")
		assert.NoError(t, err)
	}

	err = writer.Flush()
	assert.NoError(t, err)

	err = file.Close()
	assert.NoError(t, err)

	output, err = p.ReadKeyValuePairsToMap(keys)
	assert.NoError(t, err)
	assert.Equal(t, data, output)

	os.Remove(dataFileName)
}

func TestPersistence_AppendLog(t *testing.T) {
	dataFileName := "data.dat"

	keys := []string{"name", "age"}
	data := map[string]string{
		keys[0]: "khanh",
		keys[1]: "25",
	}

	p, err := NewPersistence("", dataFileName)
	assert.NoError(t, err)

	err = p.AppendKeyValuePairsMap(data)
	assert.NoError(t, err)

	file, err := os.Open(dataFileName)
	assert.NoError(t, err)

	defer file.Close()

	reader := bufio.NewReader(file)

	line, err := reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "age=25\n", line)

	line, err = reader.ReadString('\n')
	assert.NoError(t, err)
	assert.Equal(t, "name=khanh\n", line)

	os.Remove(dataFileName)
}

func TestPersistenceImpl_RenameFile(t *testing.T) {
	type fields struct {
		dataFileName   string
		dataFolderName string
	}
	type args struct {
		old string
		new string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "",
			fields: fields{
				dataFileName:   "tmp.dat",
				dataFolderName: "data/",
			},
			args: args{
				old: "tmp.dat",
				new: "wal.dat",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewPersistence(tt.fields.dataFolderName, tt.fields.dataFileName)
			assert.NoError(t, err)

			err = p.AppendStrings("a test string")
			assert.NoError(t, err)

			if err := p.RenameFile(tt.args.old, tt.args.new); (err != nil) != tt.wantErr {
				t.Errorf("PersistenceImpl.RenameFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
