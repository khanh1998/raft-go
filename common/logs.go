package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// TODO: track the log status by ID
type Log struct {
	Term        int
	ClientID    int
	SequenceNum int
	ClusterTime uint64
	Command     string
}

func (l Log) ToString() string {
	return fmt.Sprintf("%d|%d|%d|%s|%s", l.Term, l.ClientID, l.SequenceNum, strconv.FormatUint(l.ClusterTime, 10), l.Command)
}

// TODO: fix this
func NewLogFromString(s string) (Log, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 5 {
		return Log{}, errors.New("not enough token to create log")
	}

	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return Log{}, err
	}

	clientID, err := strconv.ParseInt(tokens[1], 10, 32)
	if err != nil {
		return Log{}, err
	}

	sequenceNum, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return Log{}, err
	}

	clusterTime, err := strconv.ParseUint(tokens[3], 10, 64)
	if err != nil {
		return Log{}, err
	}

	command := tokens[4]

	return Log{Term: int(term), Command: command, ClientID: int(clientID), SequenceNum: int(sequenceNum), ClusterTime: clusterTime}, nil
}
