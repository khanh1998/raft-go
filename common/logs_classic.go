package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ClassicLogFactory struct{}

func (c ClassicLogFactory) EmptySnapshot() Snapshot {
	return &ClassicSnapshot{
		LastConfig:       map[int]ClusterMember{},
		KeyValue:         map[string]string{},
		KeyLock:          map[string]int{},
		Sessions:         map[int]ClientEntry{},
		SnapshotMetadata: SnapshotMetadata{},
	}
}

func (c ClassicLogFactory) Deserialize(data []byte) (Log, error) {
	return ClassicLog{}, nil
}

func (c ClassicLogFactory) FromString(data string) (Log, error) {
	return NewLogFromString(data)
}

// attach term and time to an exist log.
func (c ClassicLogFactory) AttachTermAndTime(logI Log, term int, time uint64) (Log, error) {
	log, ok := logI.(ClassicLog)
	if !ok {
		return ClassicLog{}, errors.New("invalid log type")
	}

	log.Term = term
	log.ClusterTime = time

	return log, nil
}

func (c ClassicLogFactory) Empty() Log {
	return ClassicLog{}
}

func (c ClassicLogFactory) NoOperation(term int, time uint64) Log {
	return ClassicLog{Term: term, Command: "NO-OP", ClusterTime: time}
}

func (c ClassicLogFactory) AddNewNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log {
	return ClassicLog{
		Term:        term,
		ClusterTime: time,
		Command:     ComposeAddServerCommand(nodeId, httpUrl, rpcUrl),
	}
}

func (c ClassicLogFactory) RemoveNode(term int, time uint64, nodeId int, httpUrl string, rpcUrl string) Log {
	return ClassicLog{
		Term:        term,
		ClusterTime: time,
		Command:     ComposeRemoveServerCommand(nodeId, httpUrl, rpcUrl),
	}
}

func (c ClassicLogFactory) CreateTimeCommit(term int, nanosecond uint64) Log {
	return ClassicLog{Term: term, ClusterTime: nanosecond}
}

type ClassicLog struct {
	Term        int
	ClientID    int
	SequenceNum int
	ClusterTime uint64
	Command     string
}

func (l ClassicLog) DecomposeChangeSeverCommand() (addition bool, serverId int, httpUrl string, rpcUrl string, err error) {
	return DecomposeChangeSeverCommand(l.Command)
}

func (l ClassicLog) GetTerm() int {
	return l.Term
}

func (l ClassicLog) GetTime() uint64 {
	return l.ClusterTime
}

func (l ClassicLog) Serialize() []byte {
	return nil
}

func (l ClassicLog) ToString() string {
	return fmt.Sprintf("%d|%d|%d|%s|%s", l.Term, l.ClientID, l.SequenceNum, strconv.FormatUint(l.ClusterTime, 10), l.Command)
}

func NewLogFromString(s string) (ClassicLog, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 5 {
		return ClassicLog{}, errors.New("not enough token to create log")
	}

	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return ClassicLog{}, err
	}

	clientID, err := strconv.ParseInt(tokens[1], 10, 32)
	if err != nil {
		return ClassicLog{}, err
	}

	sequenceNum, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return ClassicLog{}, err
	}

	clusterTime, err := strconv.ParseUint(tokens[3], 10, 64)
	if err != nil {
		return ClassicLog{}, err
	}

	command := tokens[4]

	return ClassicLog{Term: int(term), Command: command, ClientID: int(clientID), SequenceNum: int(sequenceNum), ClusterTime: clusterTime}, nil
}