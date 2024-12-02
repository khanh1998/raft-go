package persistence_state

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer = otel.Tracer("persistence-state")
)

type StorageInterface interface {
	AppendWal(metadata []string, keyValuesPairs ...string) (err error)
	SaveObject(fileName string, object common.SerializableObject) error
	ReadObject(fileName string, result common.DeserializableObject) error
	StreamObject(ctx context.Context, fileName string, offset int64, maxLength int) (data []byte, eof bool, err error)
	InstallObject(ctx context.Context, fileName string, offset int64, data []byte) (err error)
	CommitObject(ctx context.Context, fileName string) (err error)
	GetObjectNames() (fileNames []string, err error)
	DeleteObject(fileName string) error
	GetWalMetadata(keys []string) (metadata [][]string, fileNames []string, err error)
	DeleteWALsOlderOrEqual(currentWAL string) error
}

// responsibility is to store Raft states like votedFor, currentTerm and logs on persistence storage
type RaftPersistenceStateImpl struct {
	votedFor    int
	currentTerm int
	logs        []common.Log

	latestSnapshot common.SnapshotMetadata // default, last log index and term are zeros
	storage        StorageInterface
	lock           sync.RWMutex
	logger         observability.Logger
	logFactory     common.LogFactory
}

type NewRaftPersistenceStateParams struct {
	VotedFor         int
	CurrentTerm      int
	Logs             []common.Log
	LogFactory       common.LogFactory
	SnapshotMetadata common.SnapshotMetadata
	Storage          StorageInterface
	Logger           observability.Logger
}

func NewRaftPersistenceState(params NewRaftPersistenceStateParams) *RaftPersistenceStateImpl {
	return &RaftPersistenceStateImpl{
		votedFor:    params.VotedFor,
		currentTerm: params.CurrentTerm,
		logs:        params.Logs,
		logFactory:  params.LogFactory,

		latestSnapshot: params.SnapshotMetadata,
		storage:        params.Storage,
		lock:           sync.RWMutex{},
		logger:         params.Logger,
	}
}

func (r *RaftPersistenceStateImpl) log() observability.Logger {
	return r.logger.With(
		"source", "raft_persistence_state",
	)
}

func (r *RaftPersistenceStateImpl) SetStorage(s StorageInterface) {
	r.storage = s
}

func (r *RaftPersistenceStateImpl) InMemoryLogLength() int {
	return len(r.logs)
}

// retain only the latest snapshot
func (r *RaftPersistenceStateImpl) cleanupSnapshot(ctx context.Context, sm common.SnapshotMetadata) (err error) {
	ctx, span := tracer.Start(ctx, "CleanupSnapshot")
	defer span.End()

	fileNames, err := r.storage.GetObjectNames()
	if err != nil {
		r.log().ErrorContext(ctx, "cleanupSnapshot", err)

		return err
	}

	deleted := []string{}

	for _, fileName := range fileNames {
		if (common.IsSnapshotFile(fileName) || common.IsTmpSnapshotFile(fileName)) &&
			fileName != sm.FileName {
			err1 := r.storage.DeleteObject(fileName)
			if err != nil {
				err = errors.Join(err1)
				span.AddEvent(
					"delete snapshot failed",
					trace.WithAttributes(
						attribute.String("error", err1.Error()),
						attribute.String("fileName", fileName),
					),
				)
			} else {
				deleted = append(deleted, fileName)
				span.AddEvent(
					"delete snapshot success",
					trace.WithAttributes(
						attribute.String("fileName", fileName),
					),
				)
			}
		}
	}

	r.log().InfoContext(ctx, "cleanupSnapshot", "deleted_snapshots", deleted, "error", err, "metadata", sm)

	return err
}

// to read snapshot as a sequence of small chunks
func (r *RaftPersistenceStateImpl) StreamSnapshot(ctx context.Context, sm common.SnapshotMetadata, offset int64, maxLength int) (data []byte, eof bool, err error) {
	fileName := sm.FileName
	return r.storage.StreamObject(ctx, fileName, offset, maxLength)
}

// to write snapshot as a sequence of small chunks
func (r *RaftPersistenceStateImpl) InstallSnapshot(ctx context.Context, fileName string, offset int64, data []byte) (err error) {
	ctx, span := tracer.Start(ctx, "InstallSnapshot")
	defer span.End()

	defer func() {
		if err != nil {
			span.AddEvent("install snapshot failed", trace.WithAttributes(
				attribute.String("fileName", fileName),
				attribute.Int64("offset", offset),
				attribute.String("error", err.Error()),
			))
		} else {
			span.AddEvent("install snapshot success", trace.WithAttributes(
				attribute.String("fileName", fileName),
				attribute.Int64("offset", offset),
			))
		}
	}()

	return r.storage.InstallObject(ctx, fileName, offset, data)
}

// snapshot will be write as a sequence of small chunks into a temporary file,
// this function will rename the temporary into a standard snapshot file name.
func (r *RaftPersistenceStateImpl) CommitSnapshot(ctx context.Context, sm common.SnapshotMetadata) (err error) {
	ctx, span := tracer.Start(ctx, "CommitSnapshot")
	defer span.End()

	defer func() {
		if err != nil {
			span.AddEvent("commit snapshot failed", trace.WithAttributes(
				attribute.String("metadata", sm.ToString()),
				attribute.String("error", err.Error()),
			))
		} else {
			span.AddEvent("commit snapshot success", trace.WithAttributes(
				attribute.String("metadata", sm.ToString()),
			))
		}
	}()

	err = r.storage.CommitObject(ctx, sm.FileName)
	if err != nil {
		return err
	}

	r.lock.Lock()

	if r.latestSnapshot == (common.SnapshotMetadata{}) {
		r.latestSnapshot = common.SnapshotMetadata{LastLogTerm: sm.LastLogTerm, LastLogIndex: sm.LastLogIndex}
	}

	r.trimPrefixLog(sm)
	r.latestSnapshot = sm

	r.lock.Unlock()

	r.log().DebugContext(ctx, "CommitSnapshot", "metadata", sm)

	go r.cleanupSnapshot(ctx, sm)
	go r.cleanupWal(ctx, sm)

	return nil
}

// save the whole snapshot into file at once
func (r *RaftPersistenceStateImpl) SaveSnapshot(ctx context.Context, snapshot common.Snapshot) (err error) {
	ctx, span := tracer.Start(ctx, "SaveSnapshot")
	defer span.End()

	sm := snapshot.Metadata()

	//snapshot.FileName = common.NewSnapshotFileName(sm.LastLogTerm, sm.LastLogIndex)

	err = r.storage.SaveObject(sm.FileName, snapshot)
	if err != nil {
		return err
	}

	r.lock.Lock()
	err = r.storage.AppendWal(r.metadata(), "snapshot", sm.ToString())
	if err != nil {
		return err
	}

	r.trimPrefixLog(sm)
	r.latestSnapshot = sm
	r.lock.Unlock()

	r.log().DebugContext(ctx, "SaveSnapshot", "metadata", sm)

	go r.cleanupSnapshot(ctx, sm)
	go r.cleanupWal(ctx, sm)

	return nil
}

func (r *RaftPersistenceStateImpl) ReadLatestSnapshot(ctx context.Context) (snap common.Snapshot, err error) {
	r.lock.RLock()
	latestSnapshot := r.latestSnapshot
	r.lock.RUnlock()

	if latestSnapshot.FileName == "" {
		return r.logFactory.EmptySnapshot(), nil
	}

	snap = r.logFactory.EmptySnapshot()

	err = r.storage.ReadObject(latestSnapshot.FileName, snap)
	if err != nil {
		return nil, err
	}

	return snap, nil
}

func (r *RaftPersistenceStateImpl) GetLog(index int) (common.Log, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	base := r.latestSnapshot.LastLogIndex

	if base == 0 && len(r.logs) == 0 {
		return r.logFactory.Empty(), common.ErrLogIsEmpty
	}

	minIdex, maxIndex := 1, base+len(r.logs)
	if index < minIdex || index > maxIndex {
		return r.logFactory.Empty(), common.ErrIndexOutOfRange
	}

	if index <= base {
		log := r.logFactory.Empty()
		var err error
		log, err = r.logFactory.AttachTermAndTime(log, r.latestSnapshot.LastLogTerm, 0)
		if err != nil {
			return log, err
		}
		return log, common.ErrLogIsInSnapshot
	}

	// valid index: base < index <= maxIndex

	adjustedIndex := index - base

	physicalIndex := adjustedIndex - 1 // Raft log index start from 1

	return r.logs[physicalIndex], nil
}

func (r *RaftPersistenceStateImpl) LogLength() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.logLength()
}

func (r *RaftPersistenceStateImpl) logLength() int {
	base := r.latestSnapshot.LastLogIndex

	return base + len(r.logs)
}

func (r *RaftPersistenceStateImpl) AppendLog(ctx context.Context, logItems []common.Log) (index int, err error) {
	keyValuePairs := []string{}
	for i := 0; i < len(logItems); i++ {
		keyValuePairs = append(keyValuePairs, "append_log", logItems[i].ToString())
	}

	r.lock.Lock()
	if err := r.storage.AppendWal(r.metadata(), keyValuePairs...); err != nil {
		return 0, err
	}

	r.logs = append(r.logs, logItems...)
	base := r.latestSnapshot.LastLogIndex
	r.lock.Unlock()

	index = base + len(r.logs)

	return index, nil
}

// delete all logs that are in memory
func (r *RaftPersistenceStateImpl) DeleteAllLog(ctx context.Context) (err error) {
	r.lock.Lock()
	deletedLogCount := len(r.logs)
	if err := r.storage.AppendWal(r.metadata(), "delete_log", strconv.Itoa(deletedLogCount)); err != nil {
		return err
	}

	r.logs = []common.Log{}
	r.lock.Unlock()

	return nil
}

func (r *RaftPersistenceStateImpl) DeleteLogFrom(ctx context.Context, index int) (deletedLogs []common.Log, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.logs) == 0 {
		return nil, common.ErrLogIsEmpty
	}

	base := r.latestSnapshot.LastLogIndex
	minIndex, maxIndex := base+1, base+len(r.logs)

	if index < minIndex || index > maxIndex {
		return nil, common.ErrIndexOutOfRange
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

// we will periodically trim the prefix of the underlying WALs,
// after the prefix WAL get trim, we will lost some information.
// so every the underlying storage create a new WAL,
// it will write some necessary data in the beginning of new WAL.
func (r *RaftPersistenceStateImpl) metadata() []string {
	index, term := r.lastLogInfo()
	return []string{
		"voted_for", strconv.Itoa(r.votedFor),
		"current_term", strconv.Itoa(r.currentTerm),
		"log_length", strconv.Itoa(r.logLength()),
		prevWalLastLogInfoKey, fmt.Sprintf("%d|%d", term, index),
	}
}

func (r *RaftPersistenceStateImpl) GetCurrentTerm() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.currentTerm
}

func (r *RaftPersistenceStateImpl) GetVotedFor() int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.votedFor
}

func (r *RaftPersistenceStateImpl) SetCurrentTerm(ctx context.Context, currentTerm int) (err error) {
	r.lock.Lock()
	err = r.storage.AppendWal(r.metadata(), "current_term", strconv.Itoa(currentTerm))
	if err != nil {
		return err
	}

	r.currentTerm = currentTerm
	r.lock.Unlock()

	return nil
}

func (r *RaftPersistenceStateImpl) SetVotedFor(ctx context.Context, votedFor int) (err error) {
	r.lock.Lock()
	err = r.storage.AppendWal(r.metadata(), "voted_for", strconv.Itoa(votedFor))
	if err != nil {
		return err
	}

	defer r.lock.Unlock()
	r.votedFor = votedFor

	return nil
}

func (r *RaftPersistenceStateImpl) GetLatestSnapshotMetadata() (snap common.SnapshotMetadata) {
	return r.latestSnapshot
}

func (r *RaftPersistenceStateImpl) trimPrefixLog(newSnapshot common.SnapshotMetadata) {
	base := r.latestSnapshot.LastLogIndex

	target := newSnapshot.LastLogIndex - base

	if target <= len(r.logs) {
		r.logs = r.logs[target:]
	}
}

func (n *RaftPersistenceStateImpl) LastLogInfo() (index, term int) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.lastLogInfo()
}

func (n *RaftPersistenceStateImpl) GetLastLog() (common.Log, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	sm := n.latestSnapshot
	base := sm.LastLogIndex

	if base == 0 && len(n.logs) == 0 {
		return n.logFactory.Empty(), errors.New("there is no log")
	}

	if base > 0 && len(n.logs) == 0 {
		return n.logFactory.Empty(), errors.New("last log is in snapshot")
	}

	if len(n.logs) > 0 {
		index := len(n.logs) - 1
		return n.logs[index], nil
	}

	return n.logFactory.Empty(), errors.New("can't get last log")
}

func (n *RaftPersistenceStateImpl) lastLogInfo() (index, term int) {
	sm := n.latestSnapshot
	base := sm.LastLogIndex

	if len(n.logs) > 0 {
		index = len(n.logs) - 1
		term = n.logs[index].GetTerm()

		return base + index + 1, term
	}

	if len(n.logs) == 0 && base > 0 {
		return sm.LastLogIndex, sm.LastLogTerm
	}

	return 0, -1
}

func (r *RaftPersistenceStateImpl) Deserialize(keyValuePairs []string, snapshot common.SnapshotMetadata) (lastLogIndex int, err error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	// index of the last logs that get recorded in the current WAL file
	// lastLogIndex = 0 if no log was found in the current wal

	var (
		prevLogIndex, prevLogTerm int // index and term of the last log of the previous WAL file
		logCount                  int // how many log have seen so far in the current WAL file
	)
	for i := 0; i < len(keyValuePairs); i += 2 {
		key, value := keyValuePairs[i], keyValuePairs[i+1]
		switch key {
		case prevWalLastLogInfoKey:
			_, err = fmt.Sscanf(value, "%d|%d", &prevLogTerm, &prevLogIndex)
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
			logItem, err := r.logFactory.FromString(value)
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
