package state_machine

import (
	"context"
	"errors"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/observability"
	"net/http"
	"strings"
	"sync"
)

const (
	ActionGet     string = "get"
	ActionSet     string = "set"
	ActionUpdate  string = "update"
	ActionCreate  string = "create"
	ActionDelete  string = "delete"
	ActionExpired string = "expired"
	ActionCAS     string = "compareAndSwap"
	ActionCAD     string = "compareAndDelete"
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
	Snapshot              gc.Snapshot
}

func NewBtreeKvStateMachine(params NewBtreeKvStateMachineParams) *BtreeKvStateMachine {

	b := &BtreeKvStateMachine{
		current:          nil,
		lock:             sync.RWMutex{},
		snapshotLock:     sync.Mutex{},
		logger:           params.Logger,
		persistenceState: params.PersistenceState,
		watcher:          NewWatcher(params.ResponseCacheCapacity),
	}
	if params.Snapshot == nil {
		b.current = NewEtcdSnapshot(params.BtreeDegree)
	} else {
		var ok bool
		b.current, ok = params.Snapshot.(*EtcdSnapshot)
		if !ok {
			b.logger.Fatal("invalid snapshot:",
				"got", params.Snapshot,
				"want", "*common.EtcdSnapshot{}",
			)
		}
	}

	return b
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

func (b *BtreeKvStateMachine) get(command common.EtcdCommand) (res common.EtcdResultRes, err error) {
	if command.Prefix {
		res := []common.KeyValue{}

		b.current.KeyValue.AscendRange(
			common.KeyValue{Key: command.Key},
			common.KeyValue{Key: command.Key + "\xFF"},
			func(item common.KeyValue) bool {
				res = append(res, item)
				return true
			},
		)

		return common.EtcdResultRes{
			Action:      ActionGet,
			Nodes:       res,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	} else {
		data, found := b.current.KeyValue.Get(common.KeyValue{Key: command.Key})
		if !found {
			return res, common.EtcdResultErr{
				Cause:     command.Key,
				ErrorCode: http.StatusNotFound,
				Index:     b.current.ChangeIndex,
				Message:   ErrKeyDoesNotExist.Error(),
			}
		}

		return common.EtcdResultRes{
			Action:      ActionGet,
			Node:        data,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	}
}

func (b *BtreeKvStateMachine) put(ctx context.Context, log common.EtcdLog, logIndex int) (result common.EtcdResultRes, err error) {
	command := log.Command
	action := getActionName(log)

	prevData, found := b.current.KeyValue.Get(common.KeyValue{Key: command.Key})

	if err := validateCommandPreconditions(prevData, found, log, logIndex); err != nil {
		return result, err
	}

	if found {
		if prevData.Expired(log.Time) && command.Refresh {
			return result, common.EtcdResultErr{
				Cause:     "refresh " + command.Key,
				ErrorCode: http.StatusNotFound,
				Index:     b.current.ChangeIndex,
				Message:   ErrKeyDoesNotExist.Error(),
			}
		}

		newData := prevData

		if command.Refresh {
			// no notification to watchers on the refreshed key
			if command.Ttl != nil && *command.Ttl > 0 {
				if newData.ExpirationTime > 0 {
					newData.ExpirationTime = log.Time + *command.Ttl
				} else {
					// the key is never expired
					// no need to refresh
					return common.EtcdResultRes{}, common.EtcdResultErr{
						Cause:     "refresh " + command.Key,
						ErrorCode: http.StatusBadRequest,
						Index:     b.current.ChangeIndex,
						Message:   "refresh TTL of a permanent key",
					}
				}
			} else {
				b.log().ErrorContext(
					ctx,
					"BtreeKvStateMachine_set",
					errors.New("refresh without ttl, check request validation"),
				)
				return common.EtcdResultRes{}, common.EtcdResultErr{
					Cause:     "refresh " + command.Key,
					ErrorCode: http.StatusInternalServerError,
					Index:     b.current.ChangeIndex,
					Message:   "refresh without ttl",
				}
			}
		} else {
			if command.Ttl != nil {
				if *command.Ttl > 0 {
					// new expiration time for the current key
					newData.ExpirationTime = log.Time + *command.Ttl
				} else {
					// unset expiration time, the current key will be never expired
					newData.ExpirationTime = 0
				}
			}
			newData.Value = *command.Value
		}

		_, newData = b.current.Update(newData, logIndex, log.GetTerm())

		return common.EtcdResultRes{
			Action:      action,
			Node:        newData,
			PrevNode:    prevData,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	} else {
		// create new key
		if command.Value == nil {
			b.log().ErrorContext(
				ctx,
				"BtreeKvStateMachine_set",
				errors.New("set a key without value, check request validation"),
			)
			return common.EtcdResultRes{}, common.EtcdResultErr{
				Cause:     "set " + command.Key,
				ErrorCode: http.StatusInternalServerError,
				Index:     b.current.ChangeIndex,
				Message:   "value is required",
			}
		}

		kv := common.KeyValue{Key: command.Key, Value: *command.Value}
		if command.Ttl != nil && *command.Ttl > 0 {
			kv.ExpirationTime = log.Time + *command.Ttl
		}
		_, data := b.current.Create(kv, logIndex, log.GetTerm())

		return common.EtcdResultRes{
			Action:      action,
			Node:        data,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	}
}

func (b *BtreeKvStateMachine) delete(log common.EtcdLog, logIndex int) (common.EtcdResultRes, error) {
	command := log.Command
	action := getActionName(log)

	if command.Prefix {
		deleteKeyVal := []common.KeyValue{}
		b.current.KeyValue.AscendRange(common.KeyValue{Key: command.Key}, common.KeyValue{Key: command.Key + "\xFF"}, func(item common.KeyValue) bool {
			deleteKeyVal = append(deleteKeyVal, item)
			return true
		})

		for _, key := range deleteKeyVal {
			b.current.DeleteKey(key.Key, logIndex, log.GetTerm())
		}

		return common.EtcdResultRes{
			Action: action,
			Node: common.KeyValue{
				Key:           command.Key,
				CreatedIndex:  b.current.ChangeIndex,
				ModifiedIndex: b.current.ChangeIndex,
			},
			PrevNodes:   deleteKeyVal,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	} else {
		prevData, found := b.current.KeyValue.Get(common.KeyValue{Key: command.Key})
		if !found {
			return common.EtcdResultRes{}, common.EtcdResultErr{
				Cause:     "delete " + command.Key,
				ErrorCode: http.StatusNotFound,
				Index:     b.current.ChangeIndex,
				Message:   ErrKeyDoesNotExist.Error(),
			}
		}

		if err := validateCommandPreconditions(prevData, found, log, logIndex); err != nil {
			return common.EtcdResultRes{}, err
		}

		b.current.DeleteKey(command.Key, logIndex, log.GetTerm())

		return common.EtcdResultRes{
			Action: action,
			Node: common.KeyValue{
				Key:           command.Key,
				CreatedIndex:  prevData.CreatedIndex,
				ModifiedIndex: b.current.ChangeIndex,
			},
			PrevNode:    prevData,
			ChangeIndex: b.current.ChangeIndex,
		}, nil
	}
}

func getActionName(log common.EtcdLog) string {
	cmd := log.Command
	method := strings.ToUpper(cmd.Action) // method is http method from http request

	switch method {
	case http.MethodPost:
		return ActionCreate
	case http.MethodDelete:
		if cmd.PrevIndex > 0 || cmd.PrevValue != nil {
			return ActionCAD
		}
		return ActionDelete
	case http.MethodGet:
		return ActionGet
	case http.MethodPut:
		if cmd.PrevExist != nil {
			if !(*cmd.PrevExist) {
				return ActionCreate
			}
			if cmd.PrevIndex <= 0 || cmd.PrevValue == nil {
				return ActionUpdate
			}
		}
		if cmd.PrevIndex > 0 || cmd.PrevValue != nil {
			return ActionCAS
		}
		return ActionSet
	}

	return ""
}

// make sure preconditions like prevExist, prevValue and prevIndex are meet
func validateCommandPreconditions(prevData common.KeyValue, found bool, log common.EtcdLog, logIndex int) error {
	command := log.Command
	if command.PrevExist != nil {
		if found != *command.PrevExist || prevData.Expired(log.Time) {
			return common.EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	if command.PrevValue != nil {
		if prevData.Value != *command.PrevValue || prevData.Expired(log.Time) {
			return common.EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	if command.PrevIndex > 0 {
		if prevData.ModifiedIndex != command.PrevIndex || prevData.Expired(log.Time) {
			return common.EtcdResultErr{
				Cause:     command.Causes(),
				ErrorCode: http.StatusBadRequest,
				Index:     logIndex,
				Message:   "compare failed",
			}
		}
	}

	return nil
}

func (b *BtreeKvStateMachine) Process(ctx context.Context, logIndex int, logI gc.Log) (result gc.LogResult, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	log := logI.(common.EtcdLog)

	defer func() {
		b.log().Debug(
			"Process",
			"log", log.ToString(),
			"result", result,
			"err", err,
		)
	}()

	command := log.Command

	if strings.EqualFold(command.Action, gc.NoOperation) {
		return result, nil
	}

	// get doesn't append a log,
	// thus doesn't provide cluster time to expire key
	if log.GetTerm() > 0 {
		resList := b.current.DeleteExpiredKeys(log.GetTime())
		for _, r := range resList {
			b.watcher.Notify(r)
		}
	}

	switch command.Action {
	case "get":
		command := log.Command
		if command.Wait {
			_, channel, err := b.watcher.Register(command.Key, command.Prefix, command.WaitIndex)
			if err != nil {
				return common.EtcdResult{}, err
			}

			return common.EtcdResult{Promise: channel}, nil
		}

		res, err := b.get(command)
		if err != nil {
			return nil, err
		}
		return common.EtcdResult{Data: res}, nil
	case "put":
		res, err := b.put(ctx, log, logIndex)
		if err != nil {
			return nil, err
		}

		if !command.Refresh {
			b.watcher.Notify(res)
		}

		return common.EtcdResult{Data: res}, nil
	case "delete":
		res, err := b.delete(log, logIndex)
		if err != nil {
			return nil, err
		}

		b.watcher.Notify(res)

		return common.EtcdResult{Data: res}, nil
	case gc.NoOperation:
		return nil, nil
	case gc.TimeCommit:
		return nil, nil
	case common.AddServer:
		_, serverId, httpUrl, rpcUrl, err := log.DecomposeChangeSeverCommand()
		if err != nil {
			return common.EtcdResult{}, common.EtcdResultErr{
				Cause:     common.AddServer,
				ErrorCode: http.StatusBadRequest,
				Index:     b.current.ChangeIndex,
				Message:   "invalid add server: %w" + err.Error(),
			}
		}

		b.current.LastConfig[serverId] = gc.ClusterMember{
			ID:      serverId,
			HttpUrl: httpUrl,
			RpcUrl:  rpcUrl,
		}
		return nil, nil
	case common.RemoveServer:
		_, serverId, _, _, err := log.DecomposeChangeSeverCommand()
		if err != nil {
			return common.EtcdResult{}, common.EtcdResultErr{
				Cause:     common.AddServer,
				ErrorCode: http.StatusBadRequest,
				Index:     b.current.ChangeIndex,
				Message:   "invalid remove server: %w" + err.Error(),
			}
		}

		delete(b.current.LastConfig, serverId)
		return nil, nil
	default:
		return result, common.EtcdResultErr{
			Cause:     command.Action,
			Message:   ErrUnsupportedCommand.Error(),
			ErrorCode: http.StatusBadRequest,
		}
	}

	return nil, nil
}
func (b *BtreeKvStateMachine) StartSnapshot(ctx context.Context) error {
	return nil
}
func (b *BtreeKvStateMachine) GetLastConfig() map[int]gc.ClusterMember {
	return nil
}
func (b *BtreeKvStateMachine) log() observability.Logger {
	sub := b.logger.With(
		"source", "btree state machine",
	)

	return sub
}
