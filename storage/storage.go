package storage

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"sort"
)

type WalMetadata struct {
	FileName     string
	LastLogIndex int
	Size         int64
}

type StorageImpl struct {
	walSizeLimit      int64
	wals              []WalMetadata
	snapshotFileNames []string
	dataFolder        string
	logger            observability.Logger
	fileUtils         FileHelper
}

type NewStorageParams struct {
	WalSize    int64 // in bytes
	DataFolder string
	Logger     observability.Logger
}

type FileHelper interface {
	ReadStrings(path string) ([]string, int64, error)
	AppendStrings(path string, lines []string) (fileSize int64, err error)
	AppendKeyValuePairs(path string, keyValues ...string) (int64, error)
	ReadKeyValuePairsToArray(path string) ([]string, int64, error)
	DeleteFile(path string) error
	GetFileNames(folder string) (names []string, err error)
	Rename(oldPath string, newPath string) (err error)
	ReadAt(path string, offset int64, maxLength int) (data []byte, eof bool, err error)
	WriteAt(path string, offset int64, data []byte) (size int, err error)
}

func NewStorageForTest(params NewStorageParams, fileUtils FileHelper) (s *StorageImpl) {
	s = &StorageImpl{
		walSizeLimit: params.WalSize,
		dataFolder:   params.DataFolder,
		logger:       params.Logger,
		fileUtils:    fileUtils,
	}

	fileNames, _ := fileUtils.GetFileNames(params.DataFolder)

	for _, fileName := range fileNames {
		if common.IsWalFile(fileName) {
			s.wals = append(s.wals, WalMetadata{FileName: fileName})
		}

		if common.IsSnapshotFile(fileName) {
			s.snapshotFileNames = append(s.snapshotFileNames, fileName)
		}
	}

	sort.Slice(s.wals, func(i, j int) bool {
		return s.wals[i].FileName < s.wals[j].FileName
	})
	sort.Strings(s.snapshotFileNames)

	if len(s.wals) == 0 {
		s.wals = append(s.wals, WalMetadata{FileName: s.nextWalFileName()})
	}

	return s
}

func NewStorage(params NewStorageParams, fileUtils FileHelper) (s *StorageImpl, err error) {
	s = &StorageImpl{
		walSizeLimit: params.WalSize,
		dataFolder:   params.DataFolder,
		logger:       params.Logger,
		fileUtils:    fileUtils,
	}

	err = common.CreateFolderIfNotExists(params.DataFolder)
	if err != nil {
		return nil, err
	}

	fileNames, err := fileUtils.GetFileNames(params.DataFolder)
	if err != nil {
		return s, fmt.Errorf("can not read dir: %w", err)
	}

	for _, fileName := range fileNames {
		if common.IsWalFile(fileName) {
			s.wals = append(s.wals, WalMetadata{FileName: fileName})
		}

		if common.IsSnapshotFile(fileName) {
			s.snapshotFileNames = append(s.snapshotFileNames, fileName)
		}
	}

	sort.Slice(s.wals, func(i, j int) bool {
		return s.wals[i].FileName < s.wals[j].FileName
	})
	sort.Strings(s.snapshotFileNames)

	if len(s.wals) == 0 {
		s.wals = append(s.wals, WalMetadata{FileName: s.nextWalFileName()})
	}

	return s, nil
}

func (s StorageImpl) nextWalFileName() (fileName string) {
	var prev string
	if len(s.wals) == 0 {
		return "wal.0000.dat"
	}
	prev = s.wals[len(s.wals)-1].FileName
	var prevCount int
	fmt.Sscanf(prev, "wal.%d.dat", &prevCount)
	return fmt.Sprintf("wal.%04d.dat", prevCount+1)
}

func (s StorageImpl) log() observability.Logger {
	return s.logger.With(
		"source", "storage",
		"wals", s.wals,
		"snapshots", s.snapshotFileNames,
	)
}

func (s StorageImpl) GetFileNames() (fileNames []string, err error) {
	return s.fileUtils.GetFileNames(s.dataFolder)
}

func (s StorageImpl) ReadAllWal() (data [][]string, err error) {
	for i := 0; i < len(s.wals); i++ {
		path := s.dataFolder + s.wals[i].FileName
		keyValuePairs, size, err := s.fileUtils.ReadKeyValuePairsToArray(path)
		if err != nil {
			return nil, err
		}
		s.wals[i].Size = size
		data = append(data, keyValuePairs)
	}

	return data, err
}

func (s *StorageImpl) newWalFile(metadata []string) (err error) {
	s.wals = append(s.wals, WalMetadata{FileName: s.nextWalFileName()})
	fileName := s.wals[len(s.wals)-1].FileName
	path := s.dataFolder + fileName
	size, err := s.fileUtils.AppendKeyValuePairs(path, metadata...)
	if err != nil {
		return err
	}

	s.wals[len(s.wals)-1].Size = size

	return nil
}

// AppendWal appends key value pairs to WAL,
// in case the current WAL size reaches the limit,
// it will create a new WAL and write metadata to the new one.
// metadata contains necessary data (voted_for and current_term) so that if the previous WALs get deleted,
// raft still can have enough data with the new WAL.
func (s *StorageImpl) AppendWal(metadata []string, keyValuesPairs ...string) (err error) {
	fileName := s.wals[len(s.wals)-1].FileName
	path := s.dataFolder + fileName
	size, err := s.fileUtils.AppendKeyValuePairs(path, keyValuesPairs...)
	if err != nil {
		return err
	}

	s.wals[len(s.wals)-1].Size = size

	if size > int64(s.walSizeLimit) {
		s.newWalFile(metadata)
	}

	return nil
}

// delete all WALs except the most recent one
func (s *StorageImpl) deleteOutdateWALs() error {
	for len(s.wals) > 1 {
		err := s.fileUtils.DeleteFile(s.wals[0].FileName)
		if err != nil {
			s.log().Error("DeleteOutdateWALs", err)
			return err
		}

		s.wals = s.wals[1:]
	}

	return nil
}

// delete any WALs that older than the currentWAL
func (s *StorageImpl) deleteWALsOlderThan(currentWAL string) error {
	// WAL file names are sorted
	for s.wals[0].FileName < currentWAL {
		err := s.fileUtils.DeleteFile(s.wals[0].FileName)
		if err != nil {
			s.log().Error("DeleteWALsOlderThan", err)
			return err
		}

		s.wals = s.wals[1:]
	}

	return nil
}

func (s *StorageImpl) deleteOutdatedSnapshots() error {
	// delete all snapshots except the most recent one
	for len(s.snapshotFileNames) > 1 {
		err := s.fileUtils.DeleteFile(s.snapshotFileNames[0])
		if err != nil {
			s.log().Error("deleteOutdateSnapshot", err)
			return err
		}

		s.snapshotFileNames = s.snapshotFileNames[1:]
	}

	return nil
}

// result should be pass as a pointer
func (s *StorageImpl) ReadObject(fileName string, result common.DeserializableObject) error {
	path := s.dataFolder + fileName
	data, _, err := s.fileUtils.ReadStrings(path)
	if err != nil {
		return err
	}

	result.Deserialize(data)

	return nil
}

func (s *StorageImpl) SaveObject(fileName string, object common.SerializableObject) error {
	path := s.dataFolder + fileName
	data := object.Serialize()

	if _, err := s.fileUtils.AppendStrings(path, data); err != nil {
		return err
	}

	err := s.fileUtils.Rename(path, path)
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageImpl) StreamObject(ctx context.Context, fileName string, offset int64, maxLength int) (data []byte, eof bool, err error) {
	path := s.dataFolder + fileName
	return s.fileUtils.ReadAt(path, offset, maxLength)
}

func (s *StorageImpl) InstallObject(ctx context.Context, fileName string, offset int64, data []byte) (err error) {
	tmpPath := s.dataFolder + "tmp." + fileName

	_, err = s.fileUtils.WriteAt(tmpPath, offset, data)
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageImpl) CommitObject(ctx context.Context, fileName string) error {
	path := s.dataFolder + fileName
	tmpPath := s.dataFolder + "tmp." + fileName

	err := s.fileUtils.Rename(tmpPath, path)
	if err != nil {
		return err
	}

	return nil
}
