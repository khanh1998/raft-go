package logic

import (
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nodeImpl_DeleteFrom(t *testing.T) {
	n := NodeImpl{Logs: []common.Log{}, DB: persistance.NewPersistenceMock()}
	err := n.DeleteLogFrom(1)
	assert.ErrorIs(t, err, ErrLogIsEmtpy)

	data := []common.Log{
		{Term: 1, Values: []common.Entry{{Key: "", Value: 1, Opcode: common.Divide}}},
		{Term: 2, Values: []common.Entry{{Key: "", Value: 2, Opcode: common.Divide}}},
		{Term: 3, Values: []common.Entry{{Key: "", Value: 3, Opcode: common.Divide}}},
	}

	n = NodeImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(4)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)
	err = n.DeleteLogFrom(0)
	assert.ErrorIs(t, err, ErrIndexOutOfRange)

	n = NodeImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(3)
	assert.NoError(t, err)
	assert.Equal(t, data[:2], n.Logs)

	n = NodeImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(2)
	assert.NoError(t, err)
	assert.Equal(t, data[:1], n.Logs)

	n = NodeImpl{Logs: make([]common.Log, 3), DB: persistance.NewPersistenceMock()}
	copy(n.Logs, data)
	err = n.DeleteLogFrom(1)
	assert.NoError(t, err)
	assert.Equal(t, []common.Log{}, n.Logs)
}
