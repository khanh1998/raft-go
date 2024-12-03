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
	LastConfig map[int]gc.ClusterMember // cluster members
	KeyValue   *btree.BTreeG[common.KeyValue]
	KeyExpire  *btree.BTreeG[common.KeyExpire]
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

func (s EtcdSnapshot) DeleteKey(key string) {
	s.KeyValue.Delete(common.KeyValue{Key: key})
	// s.common.KeyExpire.Delete(common.KeyExpire{Key: key})
	// we will not delete the corresponding data in the common.KeyExpire, it will be reconciliated later
}

func (s EtcdSnapshot) Insert(kv common.KeyValue) {
	s.KeyValue.ReplaceOrInsert(kv)
	if kv.ExpirationTime > 0 {
		s.KeyExpire.ReplaceOrInsert(common.KeyExpire{
			Key: kv.Key, ExpirationTime: kv.ExpirationTime,
			CreatedIndex: kv.CreatedIndex, ModifiedIndex: kv.ModifiedIndex,
		})
	}
}

func (s EtcdSnapshot) DeleteExpiredKeys(currTime uint64) {
	for {
		candidate, found := s.KeyExpire.Min()
		if !found {
			break
		}

		if candidate.ExpirationTime > currTime {
			break
		} else {
			s.KeyExpire.DeleteMin()
		}

		kv, found := s.KeyValue.Get(common.KeyValue{Key: candidate.Key})
		if !found {
			continue
		} else {
			// since we didn't delete the corresponding record in common.KeyExpire when we delete an item in common.KeyValue,
			// if a key is deleted, then get inserted back later, then it will have two records in common.KeyExpire,
			// this check is necessary to make sure the candidate data is not from the previous instance of the same key.
			if candidate.CreatedIndex == kv.CreatedIndex && candidate.ModifiedIndex == kv.ModifiedIndex {
				s.KeyValue.Delete(common.KeyValue{Key: kv.Key})
			}
		}
	}
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
	}
}

func (s EtcdSnapshot) Serialize() (data []byte) {
	return nil
}

func (s EtcdSnapshot) ToString() (data []string) {
	data = append(data, fmt.Sprintf("last_log_index=%d", s.LastLogIndex))
	data = append(data, fmt.Sprintf("last_log_term=%d", s.LastLogTerm))

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

	keyValueCount, err := strconv.Atoi(strings.Split(data[3], "=")[1])
	if err != nil {
		return fmt.Errorf("cannot read key-value count: %w", err)
	}

	s.LastLogIndex = lastLogIndex
	s.LastLogTerm = lastLogTerm

	i := 3
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
			s.KeyExpire.ReplaceOrInsert(common.KeyExpire{Key: kv.Key, ExpirationTime: kv.ExpirationTime, CreatedIndex: kv.CreatedIndex})
		}
	}
	return nil
}

func (s EtcdSnapshot) Deserialize(data []byte) error {
	return nil
}
