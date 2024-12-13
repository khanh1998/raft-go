package common

import (
	"fmt"
	"strconv"
	"strings"
)

type Snapshot interface {
	Metadata() SnapshotMetadata
	GetLastConfig() map[int]ClusterMember
	Copy() Snapshot
	Serialize() (data []byte)
	ToString() (data []string)
	FromString(data []string) error
	Deserialize(data []byte) error
}

type SnapshotMetadata struct {
	LastLogTerm  int
	LastLogIndex int
	LastLogTime  uint64
	FileName     string
}

func (s SnapshotMetadata) ToString() string {
	return fmt.Sprintf("%s|%d|%d|%v", s.FileName, s.LastLogIndex, s.LastLogTerm, s.LastLogTime)
}

func (s *SnapshotMetadata) FromString(str string) error {
	var err error
	tokens := strings.Split(str, "|")
	s.FileName = tokens[0]

	s.LastLogIndex, err = strconv.Atoi(tokens[1])
	if err != nil {
		return err
	}

	s.LastLogTerm, err = strconv.Atoi(tokens[2])
	if err != nil {
		return err
	}

	s.LastLogTime, err = strconv.ParseUint(tokens[3], 10, 64)
	if err != nil {
		return err
	}

	return nil
}
