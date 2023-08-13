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
	Command     any
}

func (l Log) ToString() string {
	return fmt.Sprintf("%d|%s|%d|%d", l.Term, l.Command, l.ClientID, l.SequenceNum)
}

// TODO: fix this
func NewLogFromString(s string) (Log, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 4 {
		return Log{}, errors.New("not enough token to create log")
	}

	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return Log{}, err
	}

	clientID, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return Log{}, err
	}

	sequenceNum, err := strconv.ParseInt(tokens[3], 10, 32)
	if err != nil {
		return Log{}, err
	}

	command := tokens[1]

	return Log{Term: int(term), Command: command, ClientID: int(clientID), SequenceNum: int(sequenceNum)}, nil
}
