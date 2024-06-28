package common

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrEmptyData       = errors.New("empty data")
	ErrFileExists      = errors.New("file exists")
	ErrFileNameIsEmtpy = errors.New("file name is empty")
)

// this Persistence should be shared among goroutine
type PersistenceImpl struct {
	dataFileName   string
	dataFolderName string
}

func NewPersistence(folder string, fileName string) *PersistenceImpl {
	return &PersistenceImpl{
		dataFileName:   fileName,
		dataFolderName: folder,
	}
}

func (p *PersistenceImpl) OpenFile(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer file.Close()

	p.dataFileName = fileName

	return nil
}

func (p *PersistenceImpl) CreateNewFile(fileName string) error {
	if FileExists(fileName) {
		return ErrFileExists
	}

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	defer file.Close()

	p.dataFileName = fileName

	return nil
}

func (p *PersistenceImpl) AppendLog(data map[string]string) error {
	if p.dataFileName == "" {
		return ErrFileNameIsEmtpy
	}

	path := p.dataFolderName + p.dataFileName

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	sortedKeys := make([]string, 0, len(data))
	for k := range data {
		sortedKeys = append(sortedKeys, k)
	}

	sort.Strings(sortedKeys)

	writer := bufio.NewWriter(file)
	for _, key := range sortedKeys {
		if len(key) > 0 {
			if _, err := writer.WriteString(key + "=" + data[key] + "\n"); err != nil {
				return err
			}
		}
	}

	writer.Flush()

	return nil
}

func (p *PersistenceImpl) ReadNewestLog(keys []string) (map[string]string, error) {
	if p.dataFileName == "" {
		return nil, ErrFileNameIsEmtpy
	}

	data := make(map[string]string)
	path := p.dataFolderName + p.dataFileName

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrEmptyData
		}
		return nil, err
	}

	defer file.Close()

	sort.Strings(keys)
	for _, key := range keys {
		data[key] = ""
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return data, err
		}

		tokens := strings.Split(line, "=")
		if len(tokens) == 2 {
			key, value := tokens[0], tokens[1]
			if _, ok := data[key]; ok {
				data[key] = strings.TrimSuffix(value, "\n")
			}
		}
	}

	return data, nil
}

func (p *PersistenceImpl) GetFileNames() (files []string, err error) {
	err = filepath.Walk(p.dataFolderName, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			name := info.Name()
			if IsSnapshotFile(name) {
				files = append(files, name)
			}
		}
		return nil
	})

	return
}
