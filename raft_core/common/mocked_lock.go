package common

import (
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"strconv"
	"strings"
)

type MockedLogFactory struct {
	NewSnapshot func() gc.Snapshot
}

func (c MockedLogFactory) EmptySnapshot() gc.Snapshot {
	return c.NewSnapshot()
}

func (c MockedLogFactory) Deserialize(data []byte) (gc.Log, error) {
	return MockedLog{}, nil
}

func (c MockedLogFactory) FromString(data string) (gc.Log, error) {
	return NewLogFromString(data)
}

// attach term and time to an exist log.
func (c MockedLogFactory) AttachTermAndTime(logI gc.Log, term int, time uint64) (gc.Log, error) {
	log, ok := logI.(MockedLog)
	if !ok {
		return MockedLog{}, errors.New("invalid log type")
	}

	log.Term = term
	log.ClusterTime = time

	return log, nil
}

func (c MockedLogFactory) Empty() gc.Log {
	return MockedLog{}
}

func (c MockedLogFactory) NoOperation(term int, time uint64) gc.Log {
	return MockedLog{Term: term, Command: gc.NoOperation, ClusterTime: time}
}

func (c MockedLogFactory) AddNewNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) gc.Log {
	return MockedLog{
		Term:        term,
		ClusterTime: time,
		Command:     fmt.Sprintf("addServer %d %s %s", nodeId, httpUrl, rpcUrl),
	}
}

func (c MockedLogFactory) RemoveNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) gc.Log {
	return MockedLog{
		Term:        term,
		ClusterTime: time,
		Command:     fmt.Sprintf("removeServer %d %s %s", nodeId, httpUrl, rpcUrl),
	}
}

func (c MockedLogFactory) CreateTimeCommit(term int, nanosecond uint64) gc.Log {
	return MockedLog{Term: term, ClusterTime: nanosecond, Command: gc.TimeCommit}
}

// for unit test purpose only
type MockedLog struct {
	Term        int
	ClientID    int
	SequenceNum int
	ClusterTime uint64
	Command     string
}

func (l MockedLog) DecomposeChangeSeverCommand() (addition bool, serverId int, httpUrl string, rpcUrl string, err error) {
	err = errors.New("not implemented")
	return
}

func (l MockedLog) GetTerm() int {
	return l.Term
}

func (l MockedLog) GetTime() uint64 {
	return l.ClusterTime
}

func (l MockedLog) Serialize() []byte {
	return nil
}

func (l MockedLog) ToString() string {
	return fmt.Sprintf("%d|%d|%d|%s|%s", l.Term, l.ClientID, l.SequenceNum, strconv.FormatUint(l.ClusterTime, 10), l.Command)
}

func NewLogFromString(s string) (MockedLog, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 5 {
		return MockedLog{}, errors.New("not enough token to create log")
	}

	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return MockedLog{}, err
	}

	clientID, err := strconv.ParseInt(tokens[1], 10, 32)
	if err != nil {
		return MockedLog{}, err
	}

	sequenceNum, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return MockedLog{}, err
	}

	clusterTime, err := strconv.ParseUint(tokens[3], 10, 64)
	if err != nil {
		return MockedLog{}, err
	}

	command := tokens[4]

	return MockedLog{Term: int(term), Command: command, ClientID: int(clientID), SequenceNum: int(sequenceNum), ClusterTime: clusterTime}, nil
}
