package common

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ClientEntry struct {
	LastSequenceNum int
	LastResponse    string
	ExpiryTime      uint64
	LockedKeys      map[string]struct{}
}

func (c ClientEntry) ToString(clientId int) string {
	return fmt.Sprintf("%d|%d|%d|%v", clientId, c.LastSequenceNum, c.ExpiryTime, c.LastResponse)
}

func (c *ClientEntry) FromString(str string) (clientId int, err error) {
	tokens := strings.Split(str, "|")
	clientId, err = strconv.Atoi(tokens[0])
	if err != nil {
		return clientId, err
	}

	c.LastSequenceNum, err = strconv.Atoi(tokens[1])
	if err != nil {
		return clientId, err
	}

	c.ExpiryTime, err = strconv.ParseUint(tokens[2], 10, 64)
	if err != nil {
		return clientId, err
	}

	c.LastResponse = tokens[3]

	return clientId, nil
}

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
	FileName     string
}

func (s SnapshotMetadata) ToString() string {
	return fmt.Sprintf("%s|%d|%d", s.FileName, s.LastLogIndex, s.LastLogTerm)
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

	return nil
}

type BeginSnapshotResponse struct {
	LastLogTerm  int
	LastLogIndex int
}

type SnapshotNotification struct {
	LastConfig map[int]ClusterMember // cluster members
	LastTerm   int
	LastIndex  int
}

type InstallSnapshotInput struct {
	Term      int
	LeaderId  int
	LastIndex int

	LastTerm   int
	LastConfig []ClusterMember

	FileName string
	Offset   int64
	Data     []byte

	Done bool

	Trace *RequestTraceInfo // this will be set at RPC Proxy
}

type InstallSnapshotOutput struct {
	Term    int
	Success bool   // follower response true to continue installing snapshot, false to stop
	Message string // for debugging purpose
	NodeID  int    // id of the responder
}

func NewSnapshotFileName(term, index int) string {
	return fmt.Sprintf("snapshot.%020d_%020d.dat", term, index)
}

func IsSnapshotFile(fileName string) bool {
	pattern := `^snapshot\.(\d+)_(\d+)\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}

func IsTmpSnapshotFile(fileName string) bool {
	pattern := `^tmp.snapshot\.\d+_\d+\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}

func NewWalFileName() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("snapshot.%d.dat", now)
}

func IsWalFile(fileName string) bool {
	pattern := `^wal\.\d+\.dat$`
	regex := regexp.MustCompile(pattern)

	return regex.MatchString(fileName)
}
