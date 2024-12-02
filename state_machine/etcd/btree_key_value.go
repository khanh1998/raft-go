package etcd

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	. "khanh/raft-go/common/etcd"
	"khanh/raft-go/observability"
	. "khanh/raft-go/state_machine"
	"net/http"
	"strings"
	"sync"
)

// the BTree state machine utilize B-Tree to store key value pairs,
// therefore not only support search by an exact key, but also search for a key range.
type BtreeKvStateMachine struct {
	current          *EtcdSnapshot // current snapshot of data
	lock             sync.RWMutex
	logger           observability.Logger
	snapshotLock     sync.Mutex // prevent more than one snapshot at the same time
	persistenceState RaftPersistenceState
	watcher          Watcher
}

type NewBtreeKvStateMachineParams struct {
	Logger                observability.Logger
	PersistenceState      RaftPersistenceState
	ResponseCacheCapacity int
	BtreeDegree           int
}

func NewBtreeKvStateMachine(params NewBtreeKvStateMachineParams) *BtreeKvStateMachine {
	return &BtreeKvStateMachine{
		current:          NewEtcdSnapshot(params.BtreeDegree),
		lock:             sync.RWMutex{},
		snapshotLock:     sync.Mutex{},
		logger:           params.Logger,
		persistenceState: params.PersistenceState,
		watcher:          NewWatcher(params.ResponseCacheCapacity),
	}
}

func (b *BtreeKvStateMachine) Reset(ctx context.Context) error {
	snap, err := b.persistenceState.ReadLatestSnapshot(ctx)
	if err != nil {
		return err
	}

	var ok bool
	b.current, ok = snap.(*EtcdSnapshot)
	if !ok {
		return errors.New("invalid snapshot")
	}

	return nil
}

func (b *BtreeKvStateMachine) get(command common.EtcdCommand, logIndex int) (res EtcdResultRes, err error) {
	if command.Prefix {
		res := []KeyValue{}

		b.current.KeyValue.AscendRange(
			KeyValue{Key: command.Key},
			KeyValue{Key: command.Key + "\xFF"},
			func(item KeyValue) bool {
				res = append(res, item)
				return true
			},
		)

		return EtcdResultRes{
			Action: "get",
			Nodes:  res,
		}, nil
	} else {
		data, found := b.current.KeyValue.Get(KeyValue{Key: command.Key})
		if !found {
			return res, EtcdResultErr{
				Cause:     command.Key,
				ErrorCode: http.StatusNotFound,
				Index:     logIndex,
				Message:   ErrKeyDoesNotExist.Error(),
			}
		}

		return EtcdResultRes{
			Action: "get",
			Node:   data,
		}, nil
	}
}

func (b *BtreeKvStateMachine) set(log common.EtcdLog, logIndex int) (result EtcdResultRes, err error) {
	b.current.LastLogIndex = logIndex
	b.current.LastLogTerm = log.GetTerm()
	command := log.Command

	prevData, found := b.current.KeyValue.Get(KeyValue{Key: command.Key})

	if err := validateCommandPreconditions(prevData, found, log, logIndex); err != nil {
		return result, err
	}

	if found {
		if prevData.Expired(log.Time) {
			if command.Refresh {
				return result, EtcdResultErr{
					Cause:     command.Key,
					ErrorCode: http.StatusNotFound,
					Index:     logIndex,
					Message:   ErrKeyDoesNotExist.Error(),
				}
			}
		}

		newData := prevData

		newData.ModifiedIndex = logIndex
		if command.Refresh {
			// no notification to watchers on the refreshed key
			if command.Ttl > 0 {
				newData.ExpirationTime = log.Time + command.Ttl
			} else {
				return EtcdResultRes{}, EtcdResultErr{
					Cause:     "refresh",
					ErrorCode: http.StatusBadRequest,
					Index:     logIndex,
					Message:   "refresh without ttl",
				}
			}
		} else {
			if command.Ttl > 0 {
				newData.ExpirationTime = log.Time + command.Ttl
			}
			newData.Value = *command.Value
		}

		b.current.Insert(newData)

		return EtcdResultRes{
			Action:   "set",
			Node:     newData,
			PrevNode: prevData,
		}, nil
	} else {
		if command.Refresh {
			return EtcdResultRes{}, EtcdResultErr{
				Cause:     "refresh",
				ErrorCode: http.StatusNotFound,
				Index:     logIndex,
				Message:   ErrKeyDoesNotExist.Error(),
			}
		}

		// create new key
		kv := KeyValue{Key: command.Key, Value: *command.Value, CreatedIndex: logIndex, ModifiedIndex: logIndex}
		if command.Ttl > 0 {
			kv.ExpirationTime = log.Time + command.Ttl
		}
		b.current.Insert(kv)

		return EtcdResultRes{
			Action: "set",
			Node:   kv,
		}, nil
	}
}

func (b *BtreeKvStateMachine) delete(log common.EtcdLog, logIndex int) (EtcdResultRes, error) {
	b.current.LastLogIndex = logIndex
	b.current.LastLogTerm = log.GetTerm()
	command := log.Command

	prevData, found := b.current.KeyValue.Get(KeyValue{Key: command.Key})

	if err := validateCommandPreconditions(prevData, found, log, logIndex); err != nil {
		return EtcdResultRes{}, err
	}

	if !found {
		return EtcdResultRes{
			Action: "delete",
			Node: KeyValue{
				Key:           command.Key,
				ModifiedIndex: logIndex,
				CreatedIndex:  logIndex,
			},
		}, nil
	}

	deleteKeyVal := []KeyValue{}
	if command.Prefix {
		b.current.KeyValue.AscendRange(KeyValue{Key: command.Key}, KeyValue{Key: command.Key + "\xFF"}, func(item KeyValue) bool {
			deleteKeyVal = append(deleteKeyVal, item)
			return true
		})
	} else {
		deleteKeyVal = append(deleteKeyVal, prevData)
	}

	for _, key := range deleteKeyVal {
		b.current.KeyValue.Delete(key)
	}

	if command.Prefix {
		return EtcdResultRes{
			Action: "delete",
			Node: KeyValue{
				Key:           command.Key,
				CreatedIndex:  logIndex,
				ModifiedIndex: logIndex,
			},
			PrevNodes: deleteKeyVal,
		}, nil
	} else {
		return EtcdResultRes{
			Action: "delete",
			Node: KeyValue{
				Key:           command.Key,
				CreatedIndex:  prevData.CreatedIndex,
				ModifiedIndex: logIndex,
			},
			PrevNode: prevData,
		}, nil
	}
}

// make sure preconditions like prevExist, prevValue and prevIndex are meet
func validateCommandPreconditions(prevData KeyValue, found bool, log common.EtcdLog, logIndex int) error {
	command := log.Command
	if command.PrevExist != nil {
		if found != *command.PrevExist || prevData.Expired(log.Time) {
			return EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	if command.PrevValue != nil {
		if prevData.Value != *command.PrevValue || prevData.Expired(log.Time) {
			return EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	if command.PrevIndex > 0 {
		if prevData.ModifiedIndex != command.PrevIndex || prevData.Expired(log.Time) {
			return EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	return nil
}

func (b *BtreeKvStateMachine) Process(ctx context.Context, logIndex int, logI common.Log) (result common.LogResult, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	log := logI.(common.EtcdLog)

	defer func() {
		b.log().Debug(
			"Process",
			"log", log.ToString(),
			"data", b.current.KeyValue,
		)
	}()

	command := log.Command

	if strings.EqualFold(command.Action, common.NoOperation) {
		return result, nil
	}

	switch command.Action {
	case "get":
		command := log.Command
		if command.Wait {
			_, channel, err := b.watcher.Register(command.Key, command.Prefix, command.WaitIndex)
			if err != nil {
				return EtcdResult{}, err
			}

			return EtcdResult{Promise: channel}, nil
		}

		res, err := b.get(command, logIndex)
		if err != nil {
			return nil, err
		}
		return EtcdResult{Data: res}, nil
	case "put":
		b.current.DeleteExpiredKeys(log.Time)

		res, err := b.set(log, logIndex)
		if err != nil {
			return nil, err
		}

		if !command.Refresh {
			b.watcher.Notify(res)
		}

		return EtcdResult{Data: res}, nil
	case "delete":
		b.current.DeleteExpiredKeys(log.Time)

		res, err := b.delete(log, logIndex)
		if err != nil {
			return nil, err
		}

		b.watcher.Notify(res)

		return EtcdResult{Data: res}, nil
	case common.NoOperation:
		return nil, nil
	case common.TimeCommit:
		return nil, nil
	case "addserver":
	case "removeserver":
	default:
		return result, EtcdResultErr{
			Cause:     command.Action,
			Message:   ErrUnsupportedCommand.Error(),
			ErrorCode: http.StatusBadRequest,
		}
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
