package classic

import (
	"fmt"
	"khanh/raft-go/common"
	"strconv"
	"strings"
)

type ClassicSnapshotFactory struct{}

func (c ClassicSnapshotFactory) Deserialize(data []byte) (*ClassicSnapshot, error) {
	return nil, nil
}

func (c ClassicSnapshotFactory) FromString(data []string) (*ClassicSnapshot, error) {
	lastLogIndex, err := strconv.Atoi(strings.Split(data[0], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read last log index: %w", err)
	}

	lastLogTerm, err := strconv.Atoi(strings.Split(data[1], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read last log term: %w", err)
	}

	memberCount, err := strconv.Atoi(strings.Split(data[2], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read member count: %w", err)
	}

	sessionCount, err := strconv.Atoi(strings.Split(data[3], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read session count: %w", err)
	}

	keyValueCount, err := strconv.Atoi(strings.Split(data[4], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read key-value pair count: %w", err)
	}

	keyLockCount, err := strconv.Atoi(strings.Split(data[5], "=")[1])
	if err != nil {
		return nil, fmt.Errorf("cannot read key-lock count: %w", err)
	}

	s := ClassicSnapshot{}

	s.LastLogIndex = lastLogIndex
	s.LastLogTerm = lastLogTerm

	i := 5
	for j := 0; j < memberCount; j++ {
		i++
		cm := common.ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return nil, err
		}
		s.LastConfig[cm.ID] = cm
	}

	for j := 0; j < sessionCount; j++ {
		i++
		ce := common.ClientEntry{}
		clientId, err := ce.FromString(data[i])
		if err != nil {
			return nil, err
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
			return nil, err
		}
		s.KeyLock[key] = clientId
	}

	return &s, nil
}

type ClassicSnapshot struct {
	LastConfig map[int]common.ClusterMember // cluster members
	KeyValue   map[string]string
	KeyLock    map[string]int // allowing a client session to lock a key
	Sessions   map[int]common.ClientEntry
	common.SnapshotMetadata
}

func NewClassicSnapshot() *ClassicSnapshot {
	return &ClassicSnapshot{
		LastConfig:       map[int]common.ClusterMember{},
		KeyValue:         map[string]string{},
		KeyLock:          map[string]int{},
		Sessions:         map[int]common.ClientEntry{},
		SnapshotMetadata: common.SnapshotMetadata{},
	}
}

func NewClassicSnapshotI() common.Snapshot {
	return &ClassicSnapshot{
		LastConfig:       map[int]common.ClusterMember{},
		KeyValue:         map[string]string{},
		KeyLock:          map[string]int{},
		Sessions:         map[int]common.ClientEntry{},
		SnapshotMetadata: common.SnapshotMetadata{},
	}
}

func (s ClassicSnapshot) Metadata() common.SnapshotMetadata {
	return s.SnapshotMetadata
}

func (s ClassicSnapshot) Copy() common.Snapshot {
	members := map[int]common.ClusterMember{}
	sessions := map[int]common.ClientEntry{}
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

	return &ClassicSnapshot{
		LastConfig: members,
		KeyValue:   keyValue,
		KeyLock:    keyLock,
		Sessions:   sessions,
		SnapshotMetadata: common.SnapshotMetadata{
			LastLogTerm:  s.LastLogTerm,
			LastLogIndex: s.LastLogIndex,
			FileName:     s.FileName,
		},
	}
}

func (s ClassicSnapshot) GetLastConfig() map[int]common.ClusterMember {
	return s.LastConfig
}

func (s ClassicSnapshot) ToString() (data []string) {
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

func (s ClassicSnapshot) Serialize() (data []byte) {
	return nil
}

func (s *ClassicSnapshot) Deserialize(data []byte) error {
	return nil
}

func (s *ClassicSnapshot) FromString(data []string) error {
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
		cm := common.ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return err
		}
		s.LastConfig[cm.ID] = cm
	}

	for j := 0; j < sessionCount; j++ {
		i++
		ce := common.ClientEntry{}
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
