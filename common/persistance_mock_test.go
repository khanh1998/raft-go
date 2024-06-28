package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistenceMock_AppendLog(t *testing.T) {
	mock := NewPersistenceMock()
	data := map[string]string{
		"name": "khanh",
		"age":  "25",
	}

	err := mock.AppendLog(data)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(mock.Data()))
	assert.Equal(t, "age=25\n", mock.Data()[0])
	assert.Equal(t, "name=khanh\n", mock.Data()[1])
}

func TestPersistenceMock_ReadNewestLog(t *testing.T) {
	mock := NewPersistenceMock()
	mock.SetData([]string{
		"name=khang\n",
		"age=26\n",
		"name=khanh\n",
		"age=25\n",
		"country=vietnam\n",
	})

	data, err := mock.ReadNewestLog([]string{"name", "age", "address"})
	assert.NoError(t, err)

	expectedData := map[string]string{
		"age":     "25",
		"name":    "khanh",
		"address": "",
	}
	assert.Equal(t, expectedData, data)
}
