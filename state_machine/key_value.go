package state_machine

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"sort"
	"strconv"
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

type ClientEntry struct {
	LastSequenceNum int
	LastResponse    string
	ExpiryTime      uint64
	LockedKeys      map[string]struct{}
}

// clientId|lastSequenceNum|expiryTime|lastResponse
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

type ConsensusModule interface {
	NotifyNewSnapshot(noti common.SnapshotNotification) error
	StartSnapshot(snapshotFileName string) (common.BeginSnapshotResponse, error)
	FinishSnapshot(info common.SnapshotMetadata) error
}

type Persistance interface {
	AppendKeyValuePairsArray(keyValues ...string) error
	AppendStrings(strings ...string) error
	AppendKeyValuePairsMap(data map[string]string) error
	CreateNewFile(fileName string) error
	RenameFile(oldFileName string, newFileName string) error
	ReadKeyValuePairsToMap(keys []string) (map[string]string, error)
	ReadKeyValuePairsToArray() ([]string, error)
	GetFileNames() ([]string, error)
	OpenFile(fileName string) error
}

// TODO: add session timeout mechanism
// support concurrent request
type snapshot struct {
	lastConfig map[int]common.ClusterMember // cluster members
	data       map[string]string
	lastTerm   int
	lastIndex  int
	keyLock    map[string]int // allowing a client session to lock a key
	sessions   map[int]ClientEntry
}

func newSnapshot() snapshot {
	return snapshot{
		lastConfig: make(map[int]common.ClusterMember),
		data:       make(map[string]string),
		lastTerm:   0,
		lastIndex:  0,
		keyLock:    make(map[string]int),
		sessions:   make(map[int]ClientEntry),
	}
}

func (s snapshot) copy() snapshot {
	members := map[int]common.ClusterMember{}
	sessions := map[int]ClientEntry{}
	keyValue := map[string]string{}
	keyLock := map[string]int{}

	for k, v := range s.lastConfig {
		members[k] = v
	}

	for k, v := range s.data {
		keyValue[k] = v
	}

	for k, v := range s.keyLock {
		keyLock[k] = v
	}

	for k, v := range s.sessions {
		sessions[k] = v
	}

	return snapshot{
		lastTerm:   s.lastTerm,
		lastIndex:  s.lastIndex,
		lastConfig: members,
		data:       keyValue,
		keyLock:    keyLock,
		sessions:   sessions,
	}
}

type KeyValueStateMachine struct {
	current               snapshot // live snapshot to serve the users
	previous              snapshot // the latest snapshot that got persisted in disk
	persistance           Persistance
	cm                    ConsensusModule
	lock                  sync.RWMutex
	doSnapshot            bool
	clientSessionDuration uint64 // duration in nanosecond
	logger                observability.Logger
	snapshotLock          sync.Mutex // prevent more than one snapshot at the same time
}

type NewKeyValueStateMachineParams struct {
	DB                    Persistance
	DoSnapshot            bool
	ClientSessionDuration uint64 // duration in nanosecond
	Logger                observability.Logger
}

func NewKeyValueStateMachine(params NewKeyValueStateMachineParams) (*KeyValueStateMachine, error) {
	k := &KeyValueStateMachine{
		current:               newSnapshot(),
		persistance:           params.DB,
		cm:                    nil,
		doSnapshot:            params.DoSnapshot,
		clientSessionDuration: params.ClientSessionDuration,
		logger:                params.Logger,
	}

	snapshotFile, err := k.findLatestSnapshot()
	if err != nil {
		return nil, err
	}

	if snapshotFile != "" {
		err = k.persistance.OpenFile(snapshotFile)
		if err != nil {
			return nil, err
		}

		err = k.restoreFromFile()
		if err != nil {
			return nil, err
		}
	}

	return k, nil
}

func (k *KeyValueStateMachine) log() observability.Logger {
	sub := k.logger.With(
		"source", "state machine",
	)

	return sub
}

func (k *KeyValueStateMachine) findLatestSnapshot() (fileName string, err error) {
	var snapshotFiles []string

	files, err := k.persistance.GetFileNames()
	if err != nil {
		return "", err
	}

	for _, file := range files {
		if common.IsSnapshotFile(file) {
			snapshotFiles = append(snapshotFiles, file)
		}
	}

	if len(snapshotFiles) > 0 {
		sort.Strings(snapshotFiles)
		last := len(snapshotFiles) - 1
		fileName = snapshotFiles[last]
	}

	return fileName, nil
}

func (k *KeyValueStateMachine) SetConsensusModule(cm ConsensusModule) {
	k.cm = cm
}

func (k *KeyValueStateMachine) saveSnapshotToFile(finalFileName string) error {
	data := k.serializeSnapshot()
	tmpFileName := "tmp." + finalFileName

	if err := k.persistance.CreateNewFile(tmpFileName); err != nil {
		return err
	}

	err := k.persistance.AppendStrings(data...)
	if err != nil {
		return err
	}

	// we write to a temporary file first, after finish we rename it to become the final snapshot,
	// to make sure the state machine will not read unfinished snapshot file

	err = k.persistance.RenameFile(tmpFileName, finalFileName)
	if err != nil {
		return err
	}

	return nil
}

func (k *KeyValueStateMachine) restoreFromFile() error {
	return nil
}

func (k *KeyValueStateMachine) deserializeSnapshot(data []string) error {
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

	snapshot := newSnapshot()
	snapshot.lastIndex = lastLogIndex
	snapshot.lastTerm = lastLogTerm

	i := 5
	for j := 0; j < memberCount; j++ {
		i++
		cm := common.ClusterMember{}
		err := cm.FromString(data[i])
		if err != nil {
			return err
		}
		snapshot.lastConfig[cm.ID] = cm
	}

	for j := 0; j < sessionCount; j++ {
		i++
		ce := ClientEntry{}
		clientId, err := ce.FromString(data[i])
		if err != nil {
			return err
		}
		snapshot.sessions[clientId] = ce
	}

	for j := 0; j < keyValueCount; j++ {
		i++
		tokens := strings.Split(data[i], "=")
		key, value := tokens[0], tokens[1]
		snapshot.data[key] = value
	}

	for j := 0; j < keyLockCount; j++ {
		i++
		tokens := strings.Split(data[i], "=")
		key := tokens[0]
		clientId, err := strconv.Atoi(tokens[1])
		if err != nil {
			return err
		}
		snapshot.keyLock[key] = clientId
	}

	k.current = snapshot

	return nil
}

// snapshot layout:

// 1st line:
// cluster time
// lastLogTerm, lastLogIndex

// 2nd line:
// key-value pair count, user session count, member count

// 3rd line and so on:
// key-value pairs
// user sessions
// cluster member configurations
func (k *KeyValueStateMachine) serializeSnapshot() []string {
	snapshot := k.previous
	data := []string{}

	data = append(data, fmt.Sprintf("last_log_index=%d", snapshot.lastIndex))
	data = append(data, fmt.Sprintf("last_log_term=%d", snapshot.lastTerm))

	data = append(data, fmt.Sprintf("member_count=%d", len(snapshot.lastConfig)))
	data = append(data, fmt.Sprintf("session_count=%d", len(snapshot.sessions)))
	data = append(data, fmt.Sprintf("key_value_count=%d", len(snapshot.data)))
	data = append(data, fmt.Sprintf("key_lock_count=%d", len(snapshot.keyLock)))

	for _, member := range snapshot.lastConfig {
		data = append(data, member.ToString())
	}

	for clientId, session := range snapshot.sessions {
		data = append(data, session.ToString(clientId))
	}

	for key, value := range snapshot.data {
		data = append(data, fmt.Sprintf("%s=%s", key, value))
	}

	for key, clientId := range snapshot.keyLock {
		data = append(data, fmt.Sprintf("%s=%d", key, clientId))
	}

	return data
}

func (k *KeyValueStateMachine) setSession(clientID int, sequenceNum int, response string) {
	if clientID > 0 && sequenceNum >= 0 { // when client register, clientID > 0 and sequenceNum == 0
		tmp := k.current.sessions[clientID]
		tmp.LastResponse = response
		tmp.LastSequenceNum = sequenceNum

		k.current.sessions[clientID] = tmp
	}
}

func (k *KeyValueStateMachine) Reset() error {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.current = newSnapshot()
	k.previous = snapshot{}
	// return k.saveSnapshotToFile()
	return nil
}

func (k *KeyValueStateMachine) ResetAndReloadFromFile() error {
	k.current = newSnapshot()

	return k.restoreFromFile()
}

func (k *KeyValueStateMachine) GetBase() (lastIndex int, lastTerm int) {
	return k.previous.lastIndex, k.previous.lastTerm
}

func (k *KeyValueStateMachine) InvalidateExpiredSession(clusterTime uint64) {
	expiredClientIds := map[int]struct{}{}
	for clientId, session := range k.current.sessions {
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
		session := k.current.sessions[clientId]
		for key := range session.LockedKeys {
			delete(k.current.keyLock, key)
		}
		delete(k.current.sessions, clientId)
	}

}

func (k *KeyValueStateMachine) StartSnapshot() (err error) {
	k.lock.Lock()
	k.previous = k.current.copy()
	k.lock.Unlock()

	k.snapshotLock.Lock()
	defer k.snapshotLock.Unlock()

	fileName := common.NewSnapshotFileName()
	err = k.saveSnapshotToFile(fileName)
	if err != nil {
		return err
	}
	k.cm.FinishSnapshot(common.SnapshotMetadata{
		LastLogTerm:  k.previous.lastTerm,
		LastLogIndex: k.previous.lastIndex,
		FileName:     fileName,
	})

	return nil
}

func (k *KeyValueStateMachine) Process(logIndex int, log common.Log) (result string, err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.InvalidateExpiredSession(log.ClusterTime)

	// if logIndex < k.previous.lastIndex {
	// 	return nil, ErrCommandWasSnapshot
	// }

	client, ok := k.current.sessions[log.ClientID]
	if log.ClientID > 0 && !ok {
		return "", common.ErrorSessionExpired
	}

	defer func() {
		k.setSession(log.ClientID, log.SequenceNum, result)

		k.current.lastIndex = logIndex
		k.current.lastTerm = log.Term

		k.log().Debug(
			"Process",
			"clientId", log.ClientID,
			"sequenceNum", log.SequenceNum,
			"command", log.Command,
			"logIndex", logIndex,
			"data", k.current.data,
			"cache", k.current.sessions,
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
		k.current.sessions[clientID] = ClientEntry{
			LastSequenceNum: 0, LastResponse: "", ExpiryTime: expiryTime,
			LockedKeys: map[string]struct{}{},
		}

		k.log().Debug(
			"register-session",
			"clientId", clientID,
			"expiryTime", expiryTime,
			"session", k.current.sessions[clientID],
		)

		return "", nil
	case "keep-alive":
		expiryTime := log.ClusterTime + k.clientSessionDuration
		tmp := k.current.sessions[log.ClientID]
		k.current.sessions[log.ClientID] = ClientEntry{
			LastSequenceNum: tmp.LastSequenceNum, LastResponse: tmp.LastResponse, ExpiryTime: expiryTime,
			LockedKeys: tmp.LockedKeys,
		}

		k.log().Debug(
			"keep-session",
			"clientId", log.ClientID,
			"expiryTime", expiryTime,
			"session", k.current.sessions[log.ClientID],
		)

		return "", nil
	case "addServer":
		id, httpUrl, rpcUrl, err := common.DecomposeAddServerCommand(command)
		if err != nil {
			return "", err
		}

		k.current.lastConfig[id] = common.ClusterMember{
			ID:      id,
			RpcUrl:  rpcUrl,
			HttpUrl: httpUrl,
		}

		return "", nil
	case "removeServer":
		id, _, _, err := common.DecomposeRemoveServerCommand(command)
		if err != nil {
			return "", err
		}

		delete(k.current.lastConfig, id)

		return "", nil
	}

	return "", ErrUnsupportedCommand
}

func (k *KeyValueStateMachine) get(key string) (value string, err error) {
	value, ok := k.current.data[key]
	if !ok {
		return "", ErrKeyDoesNotExist
	}

	return value, nil
}

func (k *KeyValueStateMachine) del(key string, clientId int) (err error) {
	lockClientId, ok := k.current.keyLock[key]
	if ok && lockClientId != clientId {
		return fmt.Errorf("%w, client id: %d", ErrKeyIsLocked, lockClientId)
	}

	_, ok = k.current.data[key]
	if !ok {
		return ErrKeyDoesNotExist
	}

	delete(k.current.data, key)
	delete(k.current.keyLock, key)

	session, ok := k.current.sessions[clientId]
	if ok {
		delete(session.LockedKeys, key)
		k.current.sessions[clientId] = session
	}

	return nil
}

func (k *KeyValueStateMachine) set(key string, value string, clientId int, lock bool) (string, error) {
	lockClientId, ok := k.current.keyLock[key]
	if ok && lockClientId != clientId {
		return "", fmt.Errorf("%w, client id: %d", ErrKeyIsLocked, lockClientId)
	}

	k.current.data[key] = value

	if lock && clientId == 0 {
		return "", fmt.Errorf("can't lock with client id 0 %w", ErrInvalidParameters)
	}

	if lock && clientId > 0 {
		k.current.keyLock[key] = clientId

		session, ok := k.current.sessions[clientId]
		if ok {
			session.LockedKeys[key] = struct{}{}
			k.current.sessions[clientId] = session
		}
	}

	return value, nil
}

func (k *KeyValueStateMachine) GetAll() (data map[any]any, err error) {
	data = map[any]any{}

	for key, value := range k.current.data {
		data[key] = value
	}

	return data, nil
}
