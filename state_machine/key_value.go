package state_machine

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
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
)

type ClientEntry struct {
	LastSequenceNum int
	LastResponse    any
}

type ConsensusModule interface {
	NotifyNewSnapshot(noti common.SnapshotNotification) error
}

type Persistance interface {
	AppendLog(data map[string]string) error
	CreateNewFile(fileName string) error
	ReadNewestLog(keys []string) (map[string]string, error)
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
}

func newSnapshot() snapshot {
	return snapshot{
		lastConfig: make(map[int]common.ClusterMember),
		data:       make(map[string]string),
		lastTerm:   0,
		lastIndex:  0,
	}
}

func (s snapshot) copy() snapshot {
	lastConfig := map[int]common.ClusterMember{}
	data := map[string]string{}

	for k, v := range s.lastConfig {
		lastConfig[k] = v
	}

	for k, v := range s.data {
		data[k] = v
	}

	return snapshot{
		lastTerm:   s.lastTerm,
		lastIndex:  s.lastIndex,
		lastConfig: lastConfig,
		data:       data,
	}
}

type KeyValueStateMachine struct {
	current     snapshot // live snapshot to serve the users
	previous    snapshot // the latest snapshot that got persisted in disk
	sessions    map[int]ClientEntry
	persistance Persistance
	cm          ConsensusModule
	lock        sync.RWMutex
	doSnapshot  bool
}

type NewKeyValueStateMachineParams struct {
	DB         Persistance
	DoSnapshot bool
}

func NewKeyValueStateMachine(params NewKeyValueStateMachineParams) (*KeyValueStateMachine, error) {
	k := &KeyValueStateMachine{
		current:     newSnapshot(),
		sessions:    make(map[int]ClientEntry),
		persistance: params.DB,
		cm:          nil,
		doSnapshot:  params.DoSnapshot,
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

func (k *KeyValueStateMachine) saveSnapshotToFile() error {
	data := k.takeSnapshot()
	if err := k.persistance.CreateNewFile(common.NewSnapshotFileName()); err != nil {
		return err
	}

	k.previous = k.current.copy()

	return k.persistance.AppendLog(data)
}

func (k *KeyValueStateMachine) restoreFromFile() error {
	keys, err := k.prepareKeys()
	if err != nil {
		return err
	}

	data, err := k.persistance.ReadNewestLog(keys)
	if err != nil {
		return err
	}

	return k.applySnapshot(data)
}

func (k *KeyValueStateMachine) prepareKeys() (keys []string, err error) {
	data, err := k.persistance.ReadNewestLog([]string{"log_count", "member_count"})
	if err != nil {
		return nil, err
	}

	logCount, err := strconv.Atoi(data["log_count"])
	if err != nil {
		return nil, err
	}
	memberCount, err := strconv.Atoi(data["member_count"])
	if err != nil {
		return nil, err
	}

	keys = []string{"log_count", "member_count", "last_index", "last_term"}

	for i := 0; i < memberCount; i++ {
		keys = append(keys, fmt.Sprintf("member_%d", i))
	}

	for i := 0; i < logCount; i++ {
		keys = append(keys, fmt.Sprintf("key_%d", i))
		keys = append(keys, fmt.Sprintf("value_%d", i))
	}

	return keys, nil
}

func (k *KeyValueStateMachine) applySnapshot(data map[string]string) error {

	lastIndex, err := strconv.Atoi(data["last_index"])
	if err != nil {
		return errors.Join(err, errors.New("last_index"))
	}

	lastTerm, err := strconv.Atoi(data["last_term"])
	if err != nil {
		return errors.Join(err, errors.New("last_term"))
	}

	memberCount, err := strconv.Atoi(data["member_count"])
	if err != nil {
		return errors.Join(err, errors.New("member_count"))
	}

	logCount, err := strconv.Atoi(data["log_count"])
	if err != nil {
		return errors.Join(err, errors.New("log_count"))
	}

	snapshot := snapshot{
		lastConfig: make(map[int]common.ClusterMember),
		data:       make(map[string]string),
		lastTerm:   lastTerm,
		lastIndex:  lastIndex,
	}

	for i := 0; i < memberCount; i++ {
		memStr := data[fmt.Sprintf("member_%d", i)]
		mem := common.ClusterMember{}
		if err := mem.FromString(memStr); err != nil {
			return err
		}
		snapshot.lastConfig[mem.ID] = mem
	}

	for i := 0; i < logCount; i++ {
		key := data[fmt.Sprintf("key_%d", i)]
		value := data[fmt.Sprintf("value_%d", i)]
		snapshot.data[key] = value
	}

	k.current = snapshot
	k.previous = snapshot.copy()

	return nil
}

func (k *KeyValueStateMachine) takeSnapshot() map[string]string {
	snapshot := k.current

	data := map[string]string{
		"last_index":   strconv.Itoa(snapshot.lastIndex),
		"last_term":    strconv.Itoa(snapshot.lastTerm),
		"member_count": strconv.Itoa(len(snapshot.lastConfig)),
		"log_count":    strconv.Itoa(len(snapshot.data)),
	}

	ids := []int{}
	for id := range snapshot.lastConfig {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	for id := 0; id < len(ids); id++ {
		data[fmt.Sprintf("member_%d", id)] = snapshot.lastConfig[ids[id]].ToString()
	}

	keys := []string{}
	for key := range snapshot.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i := 0; i < len(keys); i++ {
		data[fmt.Sprintf("key_%d", i)] = keys[i]
		data[fmt.Sprintf("value_%d", i)] = snapshot.data[keys[i]]
	}

	return data
}

func (k *KeyValueStateMachine) setSession(clientID int, sequenceNum int, response any) {
	if clientID > 0 && sequenceNum >= 0 { // when client register, clientID > 0 and sequenceNum == 0
		data := ClientEntry{
			LastSequenceNum: sequenceNum,
			LastResponse:    response,
		}

		k.sessions[clientID] = data
	}
}

func (k *KeyValueStateMachine) Reset() error {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.sessions = make(map[int]ClientEntry)
	k.current = newSnapshot()
	k.previous = snapshot{}
	return k.saveSnapshotToFile()
}

func (k *KeyValueStateMachine) ResetAndReloadFromFile() error {
	k.sessions = make(map[int]ClientEntry)
	k.current = snapshot{}

	return k.restoreFromFile()
}

func (k *KeyValueStateMachine) GetBase() (lastIndex int, lastTerm int) {
	return k.previous.lastIndex, k.previous.lastTerm
}

func (k *KeyValueStateMachine) Process(clientID int, sequenceNum int, commandIn any, logIndex int) (result any, err error) {
	k.lock.Lock()
	defer k.lock.Unlock()

	// if logIndex < k.previous.lastIndex {
	// 	return nil, ErrCommandWasSnapshot
	// }

	client, ok := k.sessions[clientID]
	if clientID > 0 && !ok {
		return nil, common.ErrorSessionExpired
	}

	defer func() {
		k.setSession(clientID, sequenceNum, result)

		log.Info().
			Int("client id", clientID).
			Int("sequence num", sequenceNum).
			Interface("command", commandIn).
			Int("log index", logIndex).
			Msg("Process")
		log.Info().
			Interface("data", k.current.data).
			Interface("cache", k.sessions).
			Msg("Process")

		if err == nil && k.doSnapshot {
			err1 := k.saveSnapshotToFile()
			if err != nil {
				log.Err(err1).Msg("Process_SaveSnapshotToFile")
			}
		}
	}()

	if sequenceNum > 0 && sequenceNum < client.LastSequenceNum {
		return nil, ErrorSequenceNumProcessed
	}

	if sequenceNum > 0 && sequenceNum == client.LastSequenceNum {
		return client.LastResponse, nil
	}

	command, ok := commandIn.(string)
	if !ok {
		return nil, ErrKeyMustBeString
	}

	if len(command) == 0 {
		return nil, ErrCommandIsEmpty
	}

	if strings.EqualFold(command, common.NoOperation) {
		return nil, nil
	}

	tokens := strings.Split(command, " ")
	cmd := strings.ToLower(tokens[0])

	switch cmd {
	case "get":
		if len(tokens) < 2 {
			return nil, ErrNotEnoughParameters
		}

		key := tokens[1]

		return k.get(key)
	case "set":
		if len(tokens) < 3 {
			return nil, ErrNotEnoughParameters
		}

		key := tokens[1]
		value := strings.Join(tokens[2:], " ")

		return k.set(key, value)
	case "register":
		clientID = logIndex
		sequenceNum = 0
		result = nil
		k.sessions[clientID] = ClientEntry{LastSequenceNum: 0, LastResponse: nil} // register

		return nil, nil
	case "addServer":
		id, httpUrl, rpcUrl, err := common.DecomposeAddServerCommand(command)
		if err != nil {
			return nil, err
		}

		k.current.lastConfig[id] = common.ClusterMember{
			ID:      id,
			RpcUrl:  rpcUrl,
			HttpUrl: httpUrl,
		}

		return nil, nil
	case "removeServer":
		id, _, _, err := common.DecomposeRemoveServerCommand(command)
		if err != nil {
			return nil, err
		}

		delete(k.current.lastConfig, id)

		return nil, nil
	}

	return nil, ErrUnsupportedCommand
}

func (k *KeyValueStateMachine) get(key string) (value string, err error) {
	value, ok := k.current.data[key]
	if !ok {
		return "", ErrKeyDoesNotExist
	}

	return value, nil
}

func (k *KeyValueStateMachine) set(key string, value string) (string, error) {
	k.current.data[key] = value

	return value, nil
}

func (k *KeyValueStateMachine) GetAll() (data map[any]any, err error) {
	data = map[any]any{}

	for key, value := range k.current.data {
		data[key] = value
	}

	return data, nil
}
