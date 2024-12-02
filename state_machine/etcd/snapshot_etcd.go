package etcd

import (
	"fmt"
	"khanh/raft-go/common"
	"strconv"
	"strings"

	. "khanh/raft-go/common/etcd"

	"github.com/google/btree"
)

type EtcdSnapshot struct {
	LastConfig map[int]common.ClusterMember // cluster members
	KeyValue   *btree.BTreeG[KeyValue]
	KeyExpire  *btree.BTreeG[KeyExpire]
	common.SnapshotMetadata
}

func NewEtcdSnapshot(btreeDegree int) *EtcdSnapshot {
	return &EtcdSnapshot{
		LastConfig:       make(map[int]common.ClusterMember),
		KeyValue:         btree.NewG(btreeDegree, func(a, b KeyValue) bool { return a.Key < b.Key }),
		KeyExpire:        btree.NewG(btreeDegree, func(a, b KeyExpire) bool { return a.ExpirationTime < b.ExpirationTime }),
		SnapshotMetadata: common.SnapshotMetadata{},
	}
}

func (s EtcdSnapshot) DeleteKey(key string) {
	s.KeyValue.Delete(KeyValue{Key: key})
	// s.KeyExpire.Delete(KeyExpire{Key: key})
	// we will not delete the corresponding data in the KeyExpire, it will be reconciliated later
}

func (s EtcdSnapshot) Insert(kv KeyValue) {
	s.KeyValue.ReplaceOrInsert(kv)
	if kv.ExpirationTime > 0 {
		s.KeyExpire.ReplaceOrInsert(KeyExpire{
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

		kv, found := s.KeyValue.Get(KeyValue{Key: candidate.Key})
		if !found {
			continue
		} else {
			// since we didn't delete the corresponding record in KeyExpire when we delete an item in KeyValue,
			// if a key is deleted, then get inserted back later, then it will have two records in KeyExpire,
			// this check is necessary to make sure the candidate data is not from the previous instance of the same key.
			if candidate.CreatedIndex == kv.CreatedIndex && candidate.ModifiedIndex == kv.ModifiedIndex {
				s.KeyValue.Delete(KeyValue{Key: kv.Key})
			}
		}
	}
}

func (s EtcdSnapshot) Metadata() common.SnapshotMetadata {
	return s.SnapshotMetadata
}
func (s EtcdSnapshot) GetLastConfig() map[int]common.ClusterMember {
	return s.LastConfig
}
func (s EtcdSnapshot) Copy() common.Snapshot {
	config := map[int]common.ClusterMember{}
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

	s.KeyValue.Ascend(func(item KeyValue) bool {
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
		cm := common.ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return err
		}
		s.LastConfig[cm.ID] = cm
	}

	for j := 0; j < keyValueCount; j++ {
		i++
		kv := KeyValue{}
		err := kv.FromString(data[i])
		if err != nil {
			return err
		}
		s.KeyValue.ReplaceOrInsert(kv)
		if kv.ExpirationTime > 0 {
			s.KeyExpire.ReplaceOrInsert(KeyExpire{Key: kv.Key, ExpirationTime: kv.ExpirationTime, CreatedIndex: kv.CreatedIndex})
		}
	}
	return nil
}

func (s EtcdSnapshot) Deserialize(data []byte) error {
	return nil
}
