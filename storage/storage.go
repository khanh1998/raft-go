package storage

import (
	"context"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"sort"
	"sync"
)

type WalMetadata struct {
	FileName string
	Size     int64 // to keep track size of latest WAL only
}

type StorageImpl struct {
	walSizeLimit int64
	wals         []WalMetadata
	dataFolder   string
	logger       observability.Logger
	fileUtils    FileHelper
	walLock      sync.RWMutex // for WAL files
	objLock      sync.RWMutex // for other files, consider using lock for each object
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
	CreateFile(path string) error
	ReadFirstOccurrenceKeyValuePairsToArray(path string, keys []string) ([]string, error)
}

func NewStorageForTest(params NewStorageParams, fileUtils FileHelper) (s *StorageImpl) {
	s = &StorageImpl{
		walSizeLimit: params.WalSize,
		dataFolder:   params.DataFolder,
		logger:       params.Logger,
		fileUtils:    fileUtils,
		walLock:      sync.RWMutex{},
		objLock:      sync.RWMutex{},
	}

	fileNames, _ := fileUtils.GetFileNames(params.DataFolder)

	for _, fileName := range fileNames {
		if common.IsWalFile(fileName) {
			s.wals = append(s.wals, WalMetadata{FileName: fileName})
		}
	}

	sort.Slice(s.wals, func(i, j int) bool {
		return s.wals[i].FileName < s.wals[j].FileName
	})

	if len(s.wals) == 0 {
		firstWal := s.nextWalFileName()
		err := fileUtils.CreateFile(params.DataFolder + firstWal)
		_ = err // this mock never returns error
		s.wals = append(s.wals, WalMetadata{FileName: firstWal})
	}

	return s
}

func NewStorage(params NewStorageParams, fileUtils FileHelper) (s *StorageImpl, err error) {
	s = &StorageImpl{
		walSizeLimit: params.WalSize,
		dataFolder:   params.DataFolder,
		logger:       params.Logger,
		fileUtils:    fileUtils,
		walLock:      sync.RWMutex{},
		objLock:      sync.RWMutex{},
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

	}

	sort.Slice(s.wals, func(i, j int) bool {
		return s.wals[i].FileName < s.wals[j].FileName
	})

	if len(s.wals) == 0 {
		firstWal := s.nextWalFileName()
		err := fileUtils.CreateFile(params.DataFolder + firstWal)
		if err != nil {
			return nil, err
		}
		s.wals = append(s.wals, WalMetadata{FileName: s.nextWalFileName()})
	}

	return s, nil
}

func (s *StorageImpl) nextWalFileName() (fileName string) {
	var prev string
	if len(s.wals) == 0 {
		return "wal.0000.dat"
	}
	prev = s.wals[len(s.wals)-1].FileName
	var prevCount int
	fmt.Sscanf(prev, "wal.%d.dat", &prevCount)
	return fmt.Sprintf("wal.%04d.dat", prevCount+1)
}

func (s *StorageImpl) log() observability.Logger {
	return s.logger.With(
		"source", "storage",
		"wals", s.wals,
	)
}

func (s *StorageImpl) GetObjectNames() (fileNames []string, err error) {
	s.walLock.RLock()
	s.objLock.RLock()
	defer s.walLock.RUnlock()
	defer s.objLock.RUnlock()
	return s.fileUtils.GetFileNames(s.dataFolder)
}

// iterate through list of the WAL files, and read each one from oldest to newest,
// use this when start the node
func (s *StorageImpl) WalIterator() (next func() ([]string, string, error)) {
	i := 0
	next = func() (data []string, fileName string, err error) {
		if len(s.wals) > i {
			s.walLock.RLock()
			defer s.walLock.RUnlock()

			var size int64
			fileName = s.wals[i].FileName
			path := s.dataFolder + fileName
			data, size, err = s.fileUtils.ReadKeyValuePairsToArray(path)
			if err != nil {
				return nil, "", err
			}
			s.wals[i].Size = size
			i++
			return
		} else {
			return nil, "", nil
		}
	}

	return next
}

// metadata are just normal key-value pairs that get append at the beginning of the WAL when a new WAL get created,
// so we just read the first occurrence of the key-value pairs that represent for metadata.
func (s *StorageImpl) GetWalMetadata(keys []string) (metadata [][]string, fileNames []string, err error) {
	for _, wal := range s.wals {
		path := s.dataFolder + wal.FileName

		md, err := s.fileUtils.ReadFirstOccurrenceKeyValuePairsToArray(path, keys)
		if err != nil {
			return nil, nil, err
		}

		metadata = append(metadata, md)
		fileNames = append(fileNames, wal.FileName)
	}

	return metadata, fileNames, nil
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
	s.walLock.Lock()
	defer s.walLock.Unlock()

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
func (s *StorageImpl) DeleteWALsOlderOrEqual(currentWAL string) error {
	// WAL file names are sorted
	for s.wals[0].FileName <= currentWAL {
		path := s.dataFolder + s.wals[0].FileName
		err := s.fileUtils.DeleteFile(path)
		if err != nil {
			s.log().Error("DeleteWALsOlderOrEqual", err)
			return err
		} else {
			s.log().Info("DeleteWALsOlderOrEqual", "wal", s.wals[0])
		}

		s.wals = s.wals[1:]
	}

	return nil
}

func (s *StorageImpl) DeleteObject(fileName string) error {
	s.objLock.Lock()
	defer s.objLock.Unlock()

	path := s.dataFolder + fileName
	return s.fileUtils.DeleteFile(path)
}

// result should be pass as a pointer
func (s *StorageImpl) ReadObject(fileName string, result common.DeserializableObject) error {
	s.objLock.RLock()
	defer s.objLock.RUnlock()

	path := s.dataFolder + fileName
	data, _, err := s.fileUtils.ReadStrings(path)
	if err != nil {
		return err
	}

	result.FromString(data)

	return nil
}

func (s *StorageImpl) SaveObject(fileName string, object common.SerializableObject) error {
	s.objLock.Lock()
	defer s.objLock.Unlock()

	path := s.dataFolder + fileName
	data := object.ToString()

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
	s.objLock.RLock()
	defer s.objLock.RUnlock()

	path := s.dataFolder + fileName
	return s.fileUtils.ReadAt(path, offset, maxLength)
}

func (s *StorageImpl) InstallObject(ctx context.Context, fileName string, offset int64, data []byte) (err error) {
	s.objLock.Lock()
	defer s.objLock.Unlock()

	tmpPath := s.dataFolder + "tmp." + fileName

	_, err = s.fileUtils.WriteAt(tmpPath, offset, data)
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageImpl) CommitObject(ctx context.Context, fileName string) error {
	s.objLock.Lock()
	defer s.objLock.Unlock()

	path := s.dataFolder + fileName
	tmpPath := s.dataFolder + "tmp." + fileName

	err := s.fileUtils.Rename(tmpPath, path)
	if err != nil {
		return err
	}

	return nil
}
