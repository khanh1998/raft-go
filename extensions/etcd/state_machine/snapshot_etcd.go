package state_machine

import (
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"strconv"
	"strings"

	"github.com/google/btree"
)

type EtcdSnapshot struct {
	LastConfig  map[int]gc.ClusterMember // cluster members
	KeyValue    *btree.BTreeG[common.KeyValue]
	KeyExpire   *btree.BTreeG[common.KeyExpire]
	ChangeIndex int // how many changes was applied to state machine
	gc.SnapshotMetadata
}

func NewEtcdSnapshot(btreeDegree int) *EtcdSnapshot {
	return &EtcdSnapshot{
		LastConfig:       make(map[int]gc.ClusterMember),
		KeyValue:         btree.NewG(btreeDegree, func(a, b common.KeyValue) bool { return a.Key < b.Key }),
		KeyExpire:        btree.NewG(btreeDegree, func(a, b common.KeyExpire) bool { return a.ExpirationTime < b.ExpirationTime }),
		SnapshotMetadata: gc.SnapshotMetadata{},
	}
}

func (s *EtcdSnapshot) UpdateMetadata(logTerm, logIndex int, logTime uint64) {
	s.LastLogTerm = logTerm
	s.LastLogIndex = logIndex
	s.LastLogTime = logTime
}

func (s *EtcdSnapshot) DeleteKey(key string, logIndex int, logTerm int, logTime uint64) (int, common.KeyValue) {
	s.UpdateMetadata(logTerm, logIndex, logTime)
	kv, found := s.KeyValue.Delete(common.KeyValue{Key: key})
	if found {
		s.ChangeIndex++
	}
	// s.common.KeyExpire.Delete(common.KeyExpire{Key: key})
	// we will not delete the corresponding data in the common.KeyExpire, it will be reconciliated later
	return s.ChangeIndex, kv
}

func (s *EtcdSnapshot) Update(kv common.KeyValue, logIndex int, logTerm int, logTime uint64) (int, common.KeyValue) {
	s.UpdateMetadata(logTerm, logIndex, logTime)
	s.ChangeIndex++
	kv.ModifiedIndex = s.ChangeIndex

	s.KeyValue.ReplaceOrInsert(kv)
	if kv.ExpirationTime > 0 {
		s.KeyExpire.ReplaceOrInsert(common.KeyExpire{
			Key: kv.Key, ExpirationTime: kv.ExpirationTime,
		})
	}

	return s.ChangeIndex, kv
}

func (s *EtcdSnapshot) Create(kv common.KeyValue, logIndex int, logTerm int, logTime uint64) (int, common.KeyValue) {
	s.UpdateMetadata(logTerm, logIndex, logTime)
	s.ChangeIndex++

	kv.CreatedIndex = s.ChangeIndex
	kv.ModifiedIndex = s.ChangeIndex

	s.KeyValue.ReplaceOrInsert(kv)
	if kv.ExpirationTime > 0 {
		s.KeyExpire.ReplaceOrInsert(common.KeyExpire{
			Key: kv.Key, ExpirationTime: kv.ExpirationTime,
		})
	}

	return s.ChangeIndex, kv
}

func (s *EtcdSnapshot) DeleteExpiredKeys(currTime uint64) []common.EtcdResultRes {
	deleted := []common.EtcdResultRes{}
	for {
		ke, found := s.KeyExpire.Min()
		if !found {
			break
		}

		if ke.ExpirationTime > currTime {
			break
		} else {
			s.KeyExpire.DeleteMin()
		}

		kv, found := s.KeyValue.Get(common.KeyValue{Key: ke.Key})
		if !found {
			continue
		} else {
			// zero expiration time means never expire
			if kv.ExpirationTime > 0 && kv.ExpirationTime < currTime {
				kv, found := s.KeyValue.Delete(common.KeyValue{Key: kv.Key})
				if found {
					s.ChangeIndex++
					deleted = append(deleted, common.EtcdResultRes{
						ChangeIndex: s.ChangeIndex,
						Action:      ActionExpired,
						Node:        common.KeyValue{Key: kv.Key, CreatedIndex: kv.CreatedIndex, ModifiedIndex: s.ChangeIndex},
						PrevNode:    kv,
					})
				}
			}
		}
	}

	return deleted
}

func (s EtcdSnapshot) Metadata() gc.SnapshotMetadata {
	return s.SnapshotMetadata
}
func (s EtcdSnapshot) GetLastConfig() map[int]gc.ClusterMember {
	return s.LastConfig
}
func (s EtcdSnapshot) Copy() gc.Snapshot {
	config := map[int]gc.ClusterMember{}
	for key, value := range s.LastConfig {
		config[key] = value
	}

	return &EtcdSnapshot{
		LastConfig:       config,
		KeyValue:         s.KeyValue.Clone(),
		SnapshotMetadata: s.SnapshotMetadata,
		ChangeIndex:      s.ChangeIndex,
	}
}

func (s EtcdSnapshot) Serialize() (data []byte) {
	return nil
}

func (s EtcdSnapshot) ToString() (data []string) {
	data = append(data, fmt.Sprintf("last_log_index=%d", s.LastLogIndex))
	data = append(data, fmt.Sprintf("last_log_term=%d", s.LastLogTerm))
	data = append(data, fmt.Sprintf("last_log_time=%d", s.LastLogTime))
	data = append(data, fmt.Sprintf("change_index=%d", s.ChangeIndex))

	data = append(data, fmt.Sprintf("member_count=%d", len(s.LastConfig)))
	data = append(data, fmt.Sprintf("key_value_count=%d", s.KeyValue.Len()))

	for _, member := range s.LastConfig {
		data = append(data, member.ToString())
	}

	s.KeyValue.Ascend(func(item common.KeyValue) bool {
		data = append(data, item.ToString())
		return true
	})
	return data
}

func (s *EtcdSnapshot) FromString(data []string) error {
	i := 0
	lastLogIndex, err := strconv.Atoi(strings.Split(data[0], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read last log index: %w", err)
	}

	i += 1
	lastLogTerm, err := strconv.Atoi(strings.Split(data[i], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read last log term: %w", err)
	}

	i += 1
	lastLogTime, err := strconv.ParseUint(strings.Split(data[i], "=")[1], 10, 64)
	if err != nil {
		return fmt.Errorf("cannot read last log term: %w", err)
	}

	i += 1
	changeIndex, err := strconv.Atoi(strings.Split(data[i], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read last log term: %w", err)
	}

	i += 1
	memberCount, err := strconv.Atoi(strings.Split(data[i], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read member count: %w", err)
	}

	i += 1
	keyValueCount, err := strconv.Atoi(strings.Split(data[i], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read key-value count: %w", err)
	}

	s.LastLogIndex = lastLogIndex
	s.LastLogTerm = lastLogTerm
	s.LastLogTime = lastLogTime
	s.ChangeIndex = changeIndex

	for j := 0; j < memberCount; j++ {
		i++
		cm := gc.ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return err
		}
		s.LastConfig[cm.ID] = cm
	}

	for j := 0; j < keyValueCount; j++ {
		i++
		kv := common.KeyValue{}
		err := kv.FromString(data[i])
		if err != nil {
			return err
		}
		s.KeyValue.ReplaceOrInsert(kv)
		if kv.ExpirationTime > 0 {
			s.KeyExpire.ReplaceOrInsert(common.KeyExpire{Key: kv.Key, ExpirationTime: kv.ExpirationTime})
		}
	}
	return nil
}

func (s EtcdSnapshot) Deserialize(data []byte) error {
	return nil
}
