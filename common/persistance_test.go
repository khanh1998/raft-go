package common

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistence_ReadNewestLog(t *testing.T) {
	dataFileName := "data.dat"

	keys := []string{"name", "age"}
	data := map[string]string{
		keys[0]: "khanh",
		keys[1]: "25",
	}

	p := NewPersistence("", dataFileName)
	output, err := p.ReadNewestLog(keys)
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

	output, err = p.ReadNewestLog(keys)
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

	p := NewPersistence("", dataFileName)

	err := p.AppendLog(data)
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
