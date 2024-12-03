package state_machine

import (
	"context"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/classic/common"
	"khanh/raft-go/observability"
	"strings"
	"sync"
)

// the Classic state machine utilize hashmap to store key value pairs,
// therefore it only support search by an exact key
type ClassicStateMachine struct {
	current               *ClassicSnapshot // live snapshot to serve the users
	lock                  sync.RWMutex
	clientSessionDuration uint64 // duration in nanosecond
	logger                observability.Logger
	snapshotLock          sync.Mutex // prevent more than one snapshot at the same time
	persistenceState      RaftPersistenceState
}

type NewClassicStateMachineParams struct {
	PersistState          RaftPersistenceState
	ClientSessionDuration uint64 // duration in nanosecond
	Logger                observability.Logger
	Snapshot              gc.Snapshot
}

func NewClassicStateMachine(params NewClassicStateMachineParams) *ClassicStateMachine {
	k := &ClassicStateMachine{
		clientSessionDuration: params.ClientSessionDuration,
		logger:                params.Logger,
		persistenceState:      params.PersistState,
	}

	if params.Snapshot == nil {
		k.current = NewClassicSnapshot()
	} else {
		var ok bool
		k.current, ok = params.Snapshot.(*ClassicSnapshot)
		if !ok {
			k.logger.Fatal("invalid snapshot:",
				"got", params.Snapshot,
				"want", "*common.ClassicSnapshot{}",
			)
		}
	}

	return k
}

func (k *ClassicStateMachine) GetLastConfig() map[int]gc.ClusterMember {
	return k.current.LastConfig
}

// for static cluster only
func (k *ClassicStateMachine) SetLastConfig(config map[int]gc.ClusterMember) {
	k.current.LastConfig = config
}

func (k *ClassicStateMachine) log() observability.Logger {
	sub := k.logger.With(
		"source", "state machine",
	)

	return sub
}

func (k *ClassicStateMachine) SetPersistenceStatus(ps RaftPersistenceState) {
	k.persistenceState = ps
}

func (k *ClassicStateMachine) setSession(clientID int, sequenceNum int, response gc.LogResult) {
	if clientID > 0 && sequenceNum >= 0 { // when client register, clientID > 0 and sequenceNum == 0
		tmp := k.current.Sessions[clientID]
		if response == nil {
			tmp.LastResponse = ""
		} else {
			tmp.LastResponse = response.(string)
		}
		tmp.LastSequenceNum = sequenceNum

		k.current.Sessions[clientID] = tmp
	}
}

func (k *ClassicStateMachine) Reset(ctx context.Context) (err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	snap, err := k.persistenceState.ReadLatestSnapshot(ctx)
	if err != nil {
		return err
	}
	k.current = (snap).(*ClassicSnapshot)

	return nil
}

func (k *ClassicStateMachine) InvalidateExpiredSession(clusterTime uint64) {
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

func (k *ClassicStateMachine) StartSnapshot(ctx context.Context) (err error) {
	k.lock.Lock()
	sm := k.current.Metadata()
	k.current.FileName = gc.NewSnapshotFileName(sm.LastLogTerm, sm.LastLogIndex)
	snapshot := k.current.Copy()
	k.lock.Unlock()

	go func() {
		k.snapshotLock.Lock()
		defer k.snapshotLock.Unlock()

		err = k.persistenceState.SaveSnapshot(ctx, snapshot)
		if err != nil {
			k.log().ErrorContext(ctx, "StartSnapshot_SaveSnapshot", err)
		}
	}()

	return nil
}

func (k *ClassicStateMachine) Process(ctx context.Context, logIndex int, logI gc.Log) (result gc.LogResult, err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	log, ok := logI.(common.ClassicLog)
	if !ok {
		return "", ErrInputLogTypeIsNotSupported
	}

	k.InvalidateExpiredSession(log.ClusterTime)

	// if logIndex < k.previous.lastIndex {
	// 	return nil, ErrCommandWasSnapshot
	// }

	client, ok := k.current.Sessions[log.ClientID]
	if log.ClientID > 0 && !ok {
		return "", ErrorSessionExpired
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

	if strings.EqualFold(command, gc.NoOperation) {
		return "", nil
	}
	if strings.EqualFold(command, gc.TimeCommit) {
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

		k.current.LastConfig[id] = gc.ClusterMember{
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

func (k *ClassicStateMachine) GetMembers() (members []gc.ClusterMember) {
	for _, mem := range k.current.LastConfig {
		members = append(members, mem)
	}
	return members
}

func (k *ClassicStateMachine) get(key string) (value string, err error) {
	value, ok := k.current.KeyValue[key]
	if !ok {
		return "", ErrKeyDoesNotExist
	}

	return value, nil
}

func (k *ClassicStateMachine) del(key string, clientId int) (err error) {
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

func (k *ClassicStateMachine) set(key string, value string, clientId int, lock bool) (string, error) {
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

func (k *ClassicStateMachine) GetAll() (data map[any]any, err error) {
	data = map[any]any{}

	for key, value := range k.current.KeyValue {
		data[key] = value
	}

	return data, nil
}
