package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// TODO: track the log status by ID
type Log struct {
	Term    int
	Command any
}

func (l Log) ToString() string {
	return fmt.Sprintf("%d|%s", l.Term, l.Command)
}

func NewLogFromString(s string) (Log, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 2 {
		return Log{}, errors.New("not enough token to create log")
	}

	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return Log{}, err
	}

	command := tokens[1]

	return Log{int(term), command}, nil
}
