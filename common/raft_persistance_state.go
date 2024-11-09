package common

import (
	"context"
	"fmt"
	"strconv"
	"sync"
)

type StorageInterface interface {
	AppendWal(metadata []string, keyValuesPairs ...string) (err error)
	SaveObject(fileName string, object SerializableObject) error
	ReadObject(fileName string, result DeserializableObject) error
	StreamObject(ctx context.Context, fileName string, offset int64, maxLength int) (data []byte, eof bool, err error)
	InstallObject(ctx context.Context, fileName string, offset int64, data []byte) (err error)
	CommitObject(ctx context.Context, fileName string) (err error)
}

type RaftPersistanceStateImpl struct {
	votedFor    int
	currentTerm int
	logs        []Log

	latestSnapshot SnapshotMetadata // default, last log index and term are zeros
	storage        StorageInterface
	lock           sync.RWMutex
}

type NewRaftPersistanceStateParams struct {
	VotedFor         int
	CurrentTerm      int
	Logs             []Log
	SnapshotMetadata SnapshotMetadata
	Storage          StorageInterface
}

func NewRaftPersistanceState(params NewRaftPersistanceStateParams) *RaftPersistanceStateImpl {
	return &RaftPersistanceStateImpl{
		votedFor:    params.VotedFor,
		currentTerm: params.CurrentTerm,
		logs:        params.Logs,

		latestSnapshot: params.SnapshotMetadata,
		storage:        params.Storage,
		lock:           sync.RWMutex{},
	}
}

// flushed current raft states to storage,
// use this for testing purpose only
func (r *RaftPersistanceStateImpl) Flushed() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	keyValuePairs := []string{}
	keyValuePairs = append(keyValuePairs, r.metadata()...)

	for i := 0; i < len(r.logs); i++ {
		keyValuePairs = append(keyValuePairs, "append_log", r.logs[i].ToString())
	}

	if err := r.storage.AppendWal([]string{}, keyValuePairs...); err != nil {
		return err
	}

	return nil
}

func (r *RaftPersistanceStateImpl) SetStorage(s StorageInterface) {
	r.storage = s
}

func (r *RaftPersistanceStateImpl) InMemoryLogLength() int {
	return len(r.logs)
}

func (r *RaftPersistanceStateImpl) StreamSnapshot(ctx context.Context, sm SnapshotMetadata, offset int64, maxLength int) (data []byte, eof bool, err error) {
	fileName := sm.FileName
	return r.storage.StreamObject(ctx, fileName, offset, maxLength)
}

func (r *RaftPersistanceStateImpl) InstallSnapshot(ctx context.Context, fileName string, offset int64, data []byte) (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.storage.InstallObject(ctx, fileName, offset, data)
}

func (r *RaftPersistanceStateImpl) CommitSnapshot(ctx context.Context, sm SnapshotMetadata) (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	err = r.storage.CommitObject(ctx, sm.FileName)
	if err != nil {
		return err
	}

	// should also delete all previous snapshots
	r.latestSnapshot = SnapshotMetadata{LastLogTerm: 0, LastLogIndex: 0}

	r.trimPrefixLog(sm)

	r.latestSnapshot = sm

	return nil
}

func (r *RaftPersistanceStateImpl) SaveSnapshot(ctx context.Context, snapshot *Snapshot) (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	snapshot.FileName = NewSnapshotFileName()

	err = r.storage.SaveObject(snapshot.FileName, snapshot)
	if err != nil {
		return err
	}

	sm := snapshot.Metadata()

	err = r.storage.AppendWal(r.metadata(), "snapshot", sm.ToString())
	if err != nil {
		return err
	}

	r.trimPrefixLog(sm)

	r.latestSnapshot = sm

	return nil
}

func (r *RaftPersistanceStateImpl) ReadLatestSnapshot(ctx context.Context) (snap *Snapshot, err error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.latestSnapshot.FileName == "" {
		return NewSnapshot(), nil
	}

	snap = NewSnapshot()

	err = r.storage.ReadObject(r.latestSnapshot.FileName, snap)
	if err != nil {
		return nil, err
	}

	return snap, nil
}

func (r *RaftPersistanceStateImpl) GetLog(index int) (Log, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	base := r.latestSnapshot.LastLogIndex

	if base == 0 && len(r.logs) == 0 {
		return Log{}, ErrLogIsEmpty
	}

	minIdex, maxIndex := 1, base+len(r.logs)
	if index < minIdex || index > maxIndex {
		return Log{}, ErrIndexOutOfRange
	}

	if index <= base {
		return Log{Term: r.latestSnapshot.LastLogTerm}, ErrLogIsInSnapshot
	}

	// valid index: base < index <= maxIndex

	adjustedIndex := index - base

	physicalIndex := adjustedIndex - 1 // Raft log index start from 1

	return r.logs[physicalIndex], nil
}

func (r *RaftPersistanceStateImpl) LogLength() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.logLength()
}

func (r *RaftPersistanceStateImpl) logLength() int {
	base := r.latestSnapshot.LastLogIndex

	return base + len(r.logs)
}

func (r *RaftPersistanceStateImpl) AppendLog(ctx context.Context, logItems []Log) (index int, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	keyValuePairs := []string{}
	for i := 0; i < len(logItems); i++ {
		keyValuePairs = append(keyValuePairs, "append_log", logItems[i].ToString())
	}

	if err := r.storage.AppendWal(r.metadata(), keyValuePairs...); err != nil {
		return 0, err
	}

	r.logs = append(r.logs, logItems...)

	base := r.latestSnapshot.LastLogIndex
	index = base + len(r.logs)

	return index, nil
}

// delete all logs that are in memory
func (r *RaftPersistanceStateImpl) DeleteAllLog(ctx context.Context) (err error) {
	deletedLogCount := len(r.logs)
	if err := r.storage.AppendWal(r.metadata(), "delete_log", strconv.Itoa(deletedLogCount)); err != nil {
		return err
	}

	r.logs = []Log{}
	return nil
}

func (r *RaftPersistanceStateImpl) DeleteLogFrom(ctx context.Context, index int) (deletedLogs []Log, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.logs) == 0 {
		return nil, ErrLogIsEmpty
	}

	base := r.latestSnapshot.LastLogIndex
	minIndex, maxIndex := base+1, base+len(r.logs)

	if index < minIndex || index > maxIndex {
		return nil, ErrIndexOutOfRange
	}

	physicalIndex := index - 1 // because Raft index start from 1

	// append changes to WAL
	deletedLogCount := maxIndex - physicalIndex
	if err := r.storage.AppendWal(r.metadata(), "delete_log", strconv.Itoa(deletedLogCount)); err != nil {
		return nil, err
	}

	deletedLogs = r.logs[physicalIndex:]

	r.logs = r.logs[:physicalIndex]

	return deletedLogs, nil
}

func (r *RaftPersistanceStateImpl) metadata() []string {
	index, term := r.lastLogInfo()
	return []string{
		"voted_for", strconv.Itoa(r.votedFor),
		"current_term", strconv.Itoa(r.currentTerm),
		"log_length", strconv.Itoa(r.logLength()),
		"last_log_info", fmt.Sprintf("%d|%d", index, term),
	}
}

func (r *RaftPersistanceStateImpl) GetCurrentTerm() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.currentTerm
}

func (r *RaftPersistanceStateImpl) GetVotedFor() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.votedFor
}

func (r *RaftPersistanceStateImpl) SetCurrentTerm(ctx context.Context, currentTerm int) (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	err = r.storage.AppendWal(r.metadata(), "current_term", strconv.Itoa(currentTerm))
	if err != nil {
		return err
	}

	r.currentTerm = currentTerm

	return nil
}

func (r *RaftPersistanceStateImpl) SetVotedFor(ctx context.Context, votedFor int) (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	err = r.storage.AppendWal(r.metadata(), "voted_for", strconv.Itoa(votedFor))
	if err != nil {
		return err
	}

	r.votedFor = votedFor

	return nil
}

func (r *RaftPersistanceStateImpl) GetLatestSnapshotMetadata() (snap SnapshotMetadata) {
	return r.latestSnapshot
}

func (r *RaftPersistanceStateImpl) trimPrefixLog(newSnapshot SnapshotMetadata) {
	base := r.latestSnapshot.LastLogIndex

	target := newSnapshot.LastLogIndex - base

	if target <= len(r.logs) {
		r.logs = r.logs[target:]
	}
}

func (n *RaftPersistanceStateImpl) LastLogInfo() (index, term int) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.lastLogInfo()
}

func (n *RaftPersistanceStateImpl) lastLogInfo() (index, term int) {
	sm := n.latestSnapshot
	base := sm.LastLogIndex

	if len(n.logs) > 0 {
		index = len(n.logs) - 1
		term = n.logs[index].Term

		return base + index + 1, term
	}

	if len(n.logs) == 0 && base > 0 {
		return sm.LastLogIndex, sm.LastLogTerm
	}

	return 0, -1
}

func (r *RaftPersistanceStateImpl) Deserialize(keyValuePairs []string, snapshot SnapshotMetadata) (lastLogIndex int, err error) {
	// index of the last logs that get recorded in the current WAL file
	// lastLogIndex = 0 if no log was found in the current wal

	var (
		prevLogIndex, prevLogTerm int // index and term of the last log of the previous WAL file
		logCount                  int // how many log have seen so far in the current WAL file
	)
	for i := 0; i < len(keyValuePairs); i += 2 {
		key, value := keyValuePairs[i], keyValuePairs[i+1]
		switch key {
		case "last_log_info":
			_, err = fmt.Sscanf(value, "%d|%d", &prevLogIndex, &prevLogTerm)
			if err != nil {
				return 0, err
			}

		case "current_term":
			currentTerm, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return 0, err
			}
			r.currentTerm = int(currentTerm)
		case "voted_for":
			votedFor, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return 0, err
			}
			r.votedFor = int(votedFor)
		case "append_log":
			logItem, err := NewLogFromString(value)
			if err != nil {
				return 0, err
			} else {
				logCount += 1
				logIndex := prevLogIndex + logCount
				if logIndex > snapshot.LastLogIndex {
					r.logs = append(r.logs, logItem)
				}
			}
		case "delete_log":
			deletedLogCount, err := strconv.Atoi(value)
			if err != nil {
				return 0, err
			} else {
				for i := 0; i < deletedLogCount; i++ {
					logIndex := prevLogIndex + logCount
					if logIndex > snapshot.LastLogIndex {
						length := len(r.logs)
						r.logs = r.logs[:length-1]
					}
					logCount -= 1
				}
			}
		}
	}

	lastLogIndex = prevLogIndex + logCount

	return lastLogIndex, nil
}
