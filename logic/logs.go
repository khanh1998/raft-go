package logic

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Entry struct {
	Key    string   `json:"key"`
	Value  int64    `json:"val"`
	Opcode Operator `json:"opcode"`
}

func (e Entry) ToString() string {
	return fmt.Sprintf("%s,%d,%s", e.Key, e.Value, e.Opcode)
}

func NewEntryFromString(s string) (Entry, error) {
	tokens := strings.Split(s, ",")
	key := tokens[0]
	var opcode Operator

	if tokens[2] != "" {
		opcode = Operator(tokens[2])
		if err := opcode.Valid(); err != nil {
			return Entry{}, err
		}
	}

	value, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return Entry{}, err
	}

	return Entry{key, value, opcode}, nil
}

type Log struct {
	Term   int
	Values []Entry
}

func (l Log) ToString() string {
	entryStr := ""
	for _, item := range l.Values {
		entryStr += item.ToString() + ";"
	}
	return fmt.Sprintf("%d|%s", l.Term, entryStr)
}

func NewLogFromString(s string) (Log, error) {
	tokens := strings.Split(s, "|")
	if len(tokens) != 2 {
		return Log{}, errors.New("not enough token to create log")
	}

	entries := []Entry{}
	term, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return Log{}, err
	}
	entryTokenStrs := strings.Split(tokens[1], ";")
	for _, entryTokenStr := range entryTokenStrs {
		if entryTokenStr != "" {
			entry, err := NewEntryFromString(entryTokenStr)
			if err != nil {
				return Log{}, err
			}

			entries = append(entries, entry)
		}
	}
	return Log{int(term), entries}, nil
}
