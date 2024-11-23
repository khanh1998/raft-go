package state_machine

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
)

type KeyValue struct {
	Key            string
	Value          string
	ModifiedIndex  int
	CreatedIndex   int
	ExpirationTime uint64 // cluster time
}

type Snapshot struct {
	LastConfig map[int]common.ClusterMember // cluster members
	KeyValue   *btree.BTreeG[KeyValue]
	common.SnapshotMetadata
}

type ValueUpdateSubscriber interface {
	Update(kv KeyValue)
}

type Watcher struct {
	keyWatch    map[string][]ValueUpdateSubscriber
	prefixWatch map[string][]ValueUpdateSubscriber // better use trie
	lock        sync.RWMutex
}

func (w *Watcher) Subscribe(key string, isPrefix bool, subscriber ValueUpdateSubscriber) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if isPrefix {
		w.prefixWatch[key] = append(w.keyWatch[key], subscriber)
	} else {
		w.keyWatch[key] = append(w.keyWatch[key], subscriber)
	}
}

func (w *Watcher) Notify(newData KeyValue) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	key := newData.Key
	if subscribers, ok := w.keyWatch[key]; ok {
		for _, sub := range subscribers {
			sub.Update(newData)
		}
	}

	// this approach is bad when key is long,
	// or we can search concurrently
	prefix := key
	for prefix != "" {
		if subscribers, ok := w.prefixWatch[prefix]; ok {
			for _, sub := range subscribers {
				sub.Update(newData)
			}
		}

		prefix = prefix[:len(prefix)-1] // remove last character
	}
}

type BtreeKvStateMachine struct {
	current          *Snapshot // current snapshot of data
	lock             sync.RWMutex
	logger           observability.Logger
	snapshotLock     sync.Mutex // prevent more than one snapshot at the same time
	persistenceState RaftPersistenceState
	watcher          Watcher
}

type NewBtreeKvStateMachineParams struct {
	Logger           observability.Logger
	PersistenceState RaftPersistenceState
}

func NewBtreeKvStateMachine(params NewBtreeKvStateMachineParams) *BtreeKvStateMachine {
	tree := btree.NewG(5, func(a, b KeyValue) bool {
		return a.Key < b.Key
	})
	return &BtreeKvStateMachine{
		current: &Snapshot{
			KeyValue:   tree,
			LastConfig: make(map[int]common.ClusterMember),
		},
		lock:             sync.RWMutex{},
		snapshotLock:     sync.Mutex{},
		logger:           params.Logger,
		persistenceState: params.PersistenceState,
		watcher: Watcher{
			keyWatch:    make(map[string][]ValueUpdateSubscriber),
			prefixWatch: make(map[string][]ValueUpdateSubscriber),
			lock:        sync.RWMutex{},
		},
	}
}

func (b *BtreeKvStateMachine) Reset(ctx context.Context) error {
	b.persistenceState.ReadLatestSnapshot(ctx)
	return nil
}

func (b *BtreeKvStateMachine) getByPrefix(prefix string, clusterTime uint64) (data []KeyValue) {
	b.current.KeyValue.AscendRange(KeyValue{Key: prefix}, KeyValue{Key: prefix + "\xFF"}, func(item KeyValue) bool {
		if item.ExpirationTime > 0 && item.ExpirationTime < clusterTime {
			data = append(data, item)
		} else if item.ExpirationTime > 0 {
			b.current.KeyValue.Delete(item)
		}
		return true
	})

	return data
}

func (b *BtreeKvStateMachine) get(key string, clusterTime uint64) (data KeyValue, found bool) {
	data, found = b.current.KeyValue.Get(KeyValue{Key: key})
	if !found {
		return data, false
	}

	if data.ExpirationTime > 0 && data.ExpirationTime <= clusterTime {
		b.current.KeyValue.Delete(KeyValue{Key: key})
		return data, false
	}

	return data, true
}

func (b *BtreeKvStateMachine) refreshTTL(key string, clusterTime uint64, ttl uint64) (data KeyValue, found bool) {
	data, found = b.current.KeyValue.Get(KeyValue{Key: key})
	if !found {
		return data, false
	}

	if data.ExpirationTime > 0 && data.ExpirationTime <= clusterTime {
		b.current.KeyValue.Delete(KeyValue{Key: key})
		return data, false
	}

	data.ExpirationTime = clusterTime + ttl

	return b.current.KeyValue.ReplaceOrInsert(data)
}

func (b *BtreeKvStateMachine) set(kv KeyValue, clusterTime uint64) (data KeyValue, found bool) {
	data, found = b.current.KeyValue.Get(KeyValue{Key: kv.Key})
	if !found {
		return b.current.KeyValue.ReplaceOrInsert(kv)
	}

	if data.ExpirationTime <= clusterTime {
		return b.current.KeyValue.ReplaceOrInsert(kv)
	}

	kv.CreatedIndex = data.CreatedIndex
	return b.current.KeyValue.ReplaceOrInsert(kv)
}

func (b *BtreeKvStateMachine) del(key string) (KeyValue, bool) {
	return b.current.KeyValue.Delete(KeyValue{Key: key})
}

func (b *BtreeKvStateMachine) delPrefix(prefix string) (deleted []KeyValue) {
	b.current.KeyValue.AscendRange(KeyValue{Key: prefix}, KeyValue{Key: prefix + "\xFF"}, func(item KeyValue) bool {
		deleted = append(deleted, item)
		return true
	})

	for _, key := range deleted {
		b.current.KeyValue.Delete(key)
	}
	return deleted
}

func (b *BtreeKvStateMachine) Process(ctx context.Context, logIndex int, logI common.Log) (result KeyValue, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	log := logI.(common.ClassicLog)

	defer func() {
		b.current.LastLogIndex = logIndex
		b.current.LastLogTerm = log.GetTerm()

		b.log().Debug(
			"Process",
			"log", log.ToString(),
			"data", b.current.KeyValue,
		)
	}()

	command := log.Command
	if len(command) == 0 {
		return result, ErrCommandIsEmpty
	}

	if strings.EqualFold(command, common.NoOperation) {
		return result, nil
	}

	tokens := strings.Split(command, " ")
	cmd := strings.ToLower(tokens[0])

	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return result, ErrNotEnoughParameters
		}

		key := tokens[1]

		result, found := b.get(key, log.ClusterTime)
		if !found {
			return result, ErrKeyDoesNotExist
		}
		return result, nil
	case "set":
		if len(tokens) < 3 {
			return result, ErrNotEnoughParameters
		}

		if strings.HasPrefix(tokens[1], "--ttl=") {
			t := strings.Split(tokens[1], "=")
			ttl, err := time.ParseDuration(t[1])
			if err != nil {
				return result, err
			}
			key := tokens[1]
			value := strings.Join(tokens[3:], " ")

			b.set(KeyValue{
				Key:            key,
				Value:          value,
				ModifiedIndex:  logIndex,
				CreatedIndex:   result.CreatedIndex,
				ExpirationTime: log.ClusterTime + uint64(ttl.Nanoseconds()),
			}, log.ClusterTime)
		} else {
			key := tokens[1]
			value := strings.Join(tokens[2:], " ")

			b.set(KeyValue{Key: key, Value: value, ModifiedIndex: logIndex, CreatedIndex: result.CreatedIndex}, log.ClusterTime)
		}

	case "del":
		if len(tokens) < 2 {
			return result, ErrNotEnoughParameters
		}

		key := tokens[1]

		b.del(key)
	case "addserver":
		id, httpUrl, rpcUrl, err := common.DecomposeAddServerCommand(command)
		if err != nil {
			return result, err
		}

		b.current.LastConfig[id] = common.ClusterMember{
			ID:      id,
			RpcUrl:  rpcUrl,
			HttpUrl: httpUrl,
		}

		return result, nil
	case "removeserver":
		id, _, _, err := common.DecomposeRemoveServerCommand(command)
		if err != nil {
			return result, err
		}

		delete(b.current.LastConfig, id)

		return result, nil
	default:
		return result, ErrUnsupportedCommand
	}

	return KeyValue{}, nil
}
func (b *BtreeKvStateMachine) StartSnapshot(ctx context.Context) error {
	return nil
}
func (b *BtreeKvStateMachine) GetLastConfig() map[int]common.ClusterMember {
	return nil
}
func (b *BtreeKvStateMachine) log() observability.Logger {
	sub := b.logger.With(
		"source", "btree state machine",
	)

	return sub
}
