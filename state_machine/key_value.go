package state_machine

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"strings"
	"sync"
)

var (
	ErrKeyDoesNotExist        = errors.New("key does not exist")
	ErrKeyMustBeString        = errors.New("key must be string")
	ErrValueMustBeString      = errors.New("value must be string")
	ErrCommandIsEmpty         = errors.New("command is empty")
	ErrUnsupportedCommand     = errors.New("command is unsupported")
	ErrNotEnoughParameters    = errors.New("not enough parameters")
	ErrorSequenceNumProcessed = errors.New("sequence number already processed")
	ErrCommandWasSnapshot     = errors.New("the command is included in the snapshot")
	ErrDataFileNameIsEmpty    = errors.New("data file name is empty")
	ErrKeyIsLocked            = errors.New("key is locked by another client")
	ErrInvalidParameters      = errors.New("invalid parameters")
)

type KeyValueStateMachine struct {
	current               *common.Snapshot // live snapshot to serve the users
	lock                  sync.RWMutex
	clientSessionDuration uint64 // duration in nanosecond
	logger                observability.Logger
	snapshotLock          sync.Mutex // prevent more than one snapshot at the same time
	persistanceState      RaftPersistanceState
}

type RaftPersistanceState interface {
	SaveSnapshot(ctx context.Context, snapshot *common.Snapshot) (err error)
	ReadLatestSnapshot(ctx context.Context) (snap *common.Snapshot, err error)
}

type NewKeyValueStateMachineParams struct {
	PersistState          RaftPersistanceState
	ClientSessionDuration uint64 // duration in nanosecond
	Logger                observability.Logger
	Snapshot              *common.Snapshot
}

func NewKeyValueStateMachine(params NewKeyValueStateMachineParams) *KeyValueStateMachine {
	k := &KeyValueStateMachine{
		clientSessionDuration: params.ClientSessionDuration,
		logger:                params.Logger,
		persistanceState:      params.PersistState,
	}

	if params.Snapshot == nil {
		k.current = common.NewSnapshot()
	} else {
		k.current = params.Snapshot
	}

	return k
}

func (k *KeyValueStateMachine) GetLastConfig() map[int]common.ClusterMember {
	return k.current.LastConfig
}

// for static cluster only
func (k *KeyValueStateMachine) SetLastConfig(config map[int]common.ClusterMember) {
	k.current.LastConfig = config
}

func (k *KeyValueStateMachine) log() observability.Logger {
	sub := k.logger.With(
		"source", "state machine",
	)

	return sub
}

func (k *KeyValueStateMachine) SetPersistanceStatus(ps RaftPersistanceState) {
	k.persistanceState = ps
}

func (k *KeyValueStateMachine) setSession(clientID int, sequenceNum int, response string) {
	if clientID > 0 && sequenceNum >= 0 { // when client register, clientID > 0 and sequenceNum == 0
		tmp := k.current.Sessions[clientID]
		tmp.LastResponse = response
		tmp.LastSequenceNum = sequenceNum

		k.current.Sessions[clientID] = tmp
	}
}

func (k *KeyValueStateMachine) Reset(ctx context.Context) (err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.current, err = k.persistanceState.ReadLatestSnapshot(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (k *KeyValueStateMachine) InvalidateExpiredSession(clusterTime uint64) {
	expiredClientIds := map[int]struct{}{}
	for clientId, session := range k.current.Sessions {
		if session.ExpiryTime < clusterTime {
			k.log().Debug(
				"session was expired",
				"clientId", clientId,
				"clusterTime", clusterTime,
				"session", session,
			)
			expiredClientIds[clientId] = struct{}{}
		}
	}

	for clientId := range expiredClientIds {
		session := k.current.Sessions[clientId]
		for key := range session.LockedKeys {
			delete(k.current.KeyLock, key)
		}
		delete(k.current.Sessions, clientId)
	}

}

func (k *KeyValueStateMachine) StartSnapshot(ctx context.Context) (err error) {
	k.lock.RLock()
	snapshot := k.current.Copy()
	k.lock.RUnlock()

	k.snapshotLock.Lock()
	defer k.snapshotLock.Unlock()

	err = k.persistanceState.SaveSnapshot(ctx, snapshot)
	if err != nil {
		return err
	}

	return nil
}

func (k *KeyValueStateMachine) Process(ctx context.Context, logIndex int, log common.Log) (result string, err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.InvalidateExpiredSession(log.ClusterTime)

	// if logIndex < k.previous.lastIndex {
	// 	return nil, ErrCommandWasSnapshot
	// }

	client, ok := k.current.Sessions[log.ClientID]
	if log.ClientID > 0 && !ok {
		return "", common.ErrorSessionExpired
	}

	defer func() {
		k.setSession(log.ClientID, log.SequenceNum, result)

		k.current.LastLogIndex = logIndex
		k.current.LastLogTerm = log.Term

		k.log().Debug(
			"Process",
			"clientId", log.ClientID,
			"sequenceNum", log.SequenceNum,
			"command", log.Command,
			"logIndex", logIndex,
			"data", k.current.KeyValue,
			"cache", k.current.Sessions,
		)
	}()

	if log.SequenceNum > 0 && log.SequenceNum < client.LastSequenceNum {
		return "", ErrorSequenceNumProcessed
	}

	if log.SequenceNum > 0 && log.SequenceNum == client.LastSequenceNum {
		return client.LastResponse, nil
	}

	command := log.Command
	if len(command) == 0 {
		return "", ErrCommandIsEmpty
	}

	if strings.EqualFold(command, common.NoOperation) {
		return "", nil
	}

	tokens := strings.Split(command, " ")
	cmd := strings.ToLower(tokens[0])

	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return "", ErrNotEnoughParameters
		}

		key := tokens[1]

		return k.get(key)
	case "set":
		if len(tokens) < 3 {
			return "", ErrNotEnoughParameters
		}

		// set --lock name khanh
		if tokens[1] == "--lock" {
			key := tokens[2]
			value := strings.Join(tokens[3:], " ")

			return k.set(key, value, log.ClientID, true)
		} else {
			key := tokens[1]
			value := strings.Join(tokens[2:], " ")

			return k.set(key, value, 0, false)
		}
	case "del":
		if len(tokens) < 2 {
			return "", ErrNotEnoughParameters
		}

		key := tokens[1]

		return "", k.del(key, log.ClientID)
	case "register":
		clientID := logIndex
		result = ""
		expiryTime := log.ClusterTime + k.clientSessionDuration
		k.current.Sessions[clientID] = common.ClientEntry{
			LastSequenceNum: 0, LastResponse: "", ExpiryTime: expiryTime,
			LockedKeys: map[string]struct{}{},
		}

		k.log().Debug(
			"register-session",
			"clientId", clientID,
			"expiryTime", expiryTime,
			"session", k.current.Sessions[clientID],
		)

		return "", nil
	case "keep-alive":
		expiryTime := log.ClusterTime + k.clientSessionDuration
		tmp := k.current.Sessions[log.ClientID]
		k.current.Sessions[log.ClientID] = common.ClientEntry{
			LastSequenceNum: tmp.LastSequenceNum, LastResponse: tmp.LastResponse, ExpiryTime: expiryTime,
			LockedKeys: tmp.LockedKeys,
		}

		k.log().Debug(
			"keep-session",
			"clientId", log.ClientID,
			"expiryTime", expiryTime,
			"session", k.current.Sessions[log.ClientID],
		)

		return "", nil
	case "addserver":
		id, httpUrl, rpcUrl, err := common.DecomposeAddServerCommand(command)
		if err != nil {
			return "", err
		}

		k.current.LastConfig[id] = common.ClusterMember{
			ID:      id,
			RpcUrl:  rpcUrl,
			HttpUrl: httpUrl,
		}

		return "", nil
	case "removeserver":
		id, _, _, err := common.DecomposeRemoveServerCommand(command)
		if err != nil {
			return "", err
		}

		delete(k.current.LastConfig, id)

		return "", nil
	}

	return "", ErrUnsupportedCommand
}

func (k *KeyValueStateMachine) GetMembers() (members []common.ClusterMember) {
	for _, mem := range k.current.LastConfig {
		members = append(members, mem)
	}
	return members
}

func (k *KeyValueStateMachine) get(key string) (value string, err error) {
	value, ok := k.current.KeyValue[key]
	if !ok {
		return "", ErrKeyDoesNotExist
	}

	return value, nil
}

func (k *KeyValueStateMachine) del(key string, clientId int) (err error) {
	lockClientId, ok := k.current.KeyLock[key]
	if ok && lockClientId != clientId {
		return fmt.Errorf("%w, client id: %d", ErrKeyIsLocked, lockClientId)
	}

	_, ok = k.current.KeyValue[key]
	if !ok {
		return ErrKeyDoesNotExist
	}

	delete(k.current.KeyValue, key)
	delete(k.current.KeyLock, key)

	session, ok := k.current.Sessions[clientId]
	if ok {
		delete(session.LockedKeys, key)
		k.current.Sessions[clientId] = session
	}

	return nil
}

func (k *KeyValueStateMachine) set(key string, value string, clientId int, lock bool) (string, error) {
	lockClientId, ok := k.current.KeyLock[key]
	if ok && lockClientId != clientId {
		return "", fmt.Errorf("%w, client id: %d", ErrKeyIsLocked, lockClientId)
	}

	k.current.KeyValue[key] = value

	if lock && clientId == 0 {
		return "", fmt.Errorf("can't lock with client id 0 %w", ErrInvalidParameters)
	}

	if lock && clientId > 0 {
		k.current.KeyLock[key] = clientId

		session, ok := k.current.Sessions[clientId]
		if ok {
			session.LockedKeys[key] = struct{}{}
			k.current.Sessions[clientId] = session
		}
	}

	return value, nil
}

func (k *KeyValueStateMachine) GetAll() (data map[any]any, err error) {
	data = map[any]any{}

	for key, value := range k.current.KeyValue {
		data[key] = value
	}

	return data, nil
}
