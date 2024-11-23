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

type KeyValueData struct {
	Value         string
	CreatedIndex  int
	ModifiedIndex int
}

type SnapshotV2 struct {
	LastConfig map[int]ClusterMember // cluster members
	KeyValue   map[string]KeyValueData
	Sessions   map[int]ClientEntry
	SnapshotMetadata
}

type Snapshot struct {
	LastConfig map[int]ClusterMember // cluster members
	KeyValue   map[string]string
	KeyLock    map[string]int // allowing a client session to lock a key
	Sessions   map[int]ClientEntry
	SnapshotMetadata
}

func NewSnapshot() *Snapshot {
	return &Snapshot{
		LastConfig:       map[int]ClusterMember{},
		KeyValue:         map[string]string{},
		KeyLock:          map[string]int{},
		Sessions:         map[int]ClientEntry{},
		SnapshotMetadata: SnapshotMetadata{},
	}
}

func (s Snapshot) Metadata() SnapshotMetadata {
	return s.SnapshotMetadata
}

func (s Snapshot) Copy() *Snapshot {
	members := map[int]ClusterMember{}
	sessions := map[int]ClientEntry{}
	keyValue := map[string]string{}
	keyLock := map[string]int{}

	for k, v := range s.LastConfig {
		members[k] = v
	}

	for k, v := range s.KeyValue {
		keyValue[k] = v
	}

	for k, v := range s.KeyLock {
		keyLock[k] = v
	}

	for k, v := range s.Sessions {
		sessions[k] = v
	}

	return &Snapshot{
		LastConfig: members,
		KeyValue:   keyValue,
		KeyLock:    keyLock,
		Sessions:   sessions,
		SnapshotMetadata: SnapshotMetadata{
			LastLogTerm:  s.LastLogTerm,
			LastLogIndex: s.LastLogIndex,
			FileName:     s.FileName,
		},
	}
}

func (s *Snapshot) Deserialize(data []string) (err error) {
	lastLogIndex, err := strconv.Atoi(strings.Split(data[0], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read last log index: %w", err)
	}

	lastLogTerm, err := strconv.Atoi(strings.Split(data[1], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read last log term: %w", err)
	}

	memberCount, err := strconv.Atoi(strings.Split(data[2], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read member count: %w", err)
	}

	sessionCount, err := strconv.Atoi(strings.Split(data[3], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read session count: %w", err)
	}

	keyValueCount, err := strconv.Atoi(strings.Split(data[4], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read key-value pair count: %w", err)
	}

	keyLockCount, err := strconv.Atoi(strings.Split(data[5], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read key-lock count: %w", err)
	}

	s.LastLogIndex = lastLogIndex
	s.LastLogTerm = lastLogTerm

	i := 5
	for j := 0; j < memberCount; j++ {
		i++
		cm := ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return err
		}
		s.LastConfig[cm.ID] = cm
	}

	for j := 0; j < sessionCount; j++ {
		i++
		ce := ClientEntry{}
		clientId, err := ce.FromString(data[i])
		if err != nil {
			return err
		}
		s.Sessions[clientId] = ce
	}

	for j := 0; j < keyValueCount; j++ {
		i++
		tokens := strings.Split(data[i], "=")
		key, value := tokens[0], tokens[1]
		s.KeyValue[key] = value
	}

	for j := 0; j < keyLockCount; j++ {
		i++
		tokens := strings.Split(data[i], "=")
		key := tokens[0]
		clientId, err := strconv.Atoi(tokens[1])
		if err != nil {
			return err
		}
		s.KeyLock[key] = clientId
	}

	return nil
}

func (s Snapshot) Serialize() (data []string) {
	data = append(data, fmt.Sprintf("last_log_index=%d", s.LastLogIndex))
	data = append(data, fmt.Sprintf("last_log_term=%d", s.LastLogTerm))

	data = append(data, fmt.Sprintf("member_count=%d", len(s.LastConfig)))
	data = append(data, fmt.Sprintf("session_count=%d", len(s.Sessions)))
	data = append(data, fmt.Sprintf("key_value_count=%d", len(s.KeyValue)))
	data = append(data, fmt.Sprintf("key_lock_count=%d", len(s.KeyLock)))

	for _, member := range s.LastConfig {
		data = append(data, member.ToString())
	}

	for clientId, session := range s.Sessions {
		data = append(data, session.ToString(clientId))
	}

	for key, value := range s.KeyValue {
		data = append(data, fmt.Sprintf("%s=%s", key, value))
	}

	for key, clientId := range s.KeyLock {
		data = append(data, fmt.Sprintf("%s=%d", key, clientId))
	}

	return data
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
