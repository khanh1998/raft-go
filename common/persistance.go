package common

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrEmptyData        = errors.New("empty data")
	ErrFileExists       = errors.New("file exists")
	ErrFileDoesNotExist = errors.New("file doesn't exist")
	ErrFileNameIsEmpty  = errors.New("file name is empty")
)

// this Persistence should be shared among goroutine
type PersistenceImpl struct {
	dataFileName   string
	dataFolderName string
}

func NewPersistence(folder string, fileName string) (*PersistenceImpl, error) {
	if folder != "" {
		if err := CreateFolderIfNotExists(folder); err != nil {
			return nil, err
		}
	}

	return &PersistenceImpl{
		dataFileName:   fileName,
		dataFolderName: folder,
	}, nil
}

func (p *PersistenceImpl) OpenFile(fileName string) error {
	path := p.dataFolderName + fileName

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	defer file.Close()

	p.dataFileName = fileName

	return nil
}

func (p *PersistenceImpl) DeleteFile(fileName string) error {
	return nil
}

func (p *PersistenceImpl) RenameFile(old string, new string) error {
	oldPath, newPath := p.dataFolderName+old, p.dataFolderName+new

	if !FileExists(oldPath) {
		return fmt.Errorf("file %s %w", oldPath, ErrFileDoesNotExist)
	}

	if FileExists(newPath) {
		return fmt.Errorf("file %s %w", newPath, ErrFileExists)
	}

	return os.Rename(oldPath, newPath)
}

func (p *PersistenceImpl) CreateNewFile(fileName string) error {
	path := p.dataFolderName + fileName
	if FileExists(path) {
		return ErrFileExists
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	p.dataFileName = fileName

	return nil
}

func (p *PersistenceImpl) AppendStrings(lines ...string) error {
	if p.dataFileName == "" {
		return ErrFileNameIsEmpty
	}

	path := p.dataFolderName + p.dataFileName

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	content := ""
	for _, line := range lines {
		content += line + "\n"
	}

	writer := bufio.NewWriter(file)
	if _, err := writer.WriteString(content); err != nil {
		return err
	}

	return writer.Flush()
}

func (p *PersistenceImpl) AppendKeyValuePairsArray(keyValues ...string) error {
	length := len(keyValues)
	if length%2 != 0 {
		return fmt.Errorf("length of input array must be even: %d", len(keyValues))
	}

	if p.dataFileName == "" {
		return ErrFileNameIsEmpty
	}

	path := p.dataFolderName + p.dataFileName

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	content := ""
	for i := 0; i < length; i += 2 {
		key, value := keyValues[i], keyValues[i+1]
		if len(key) > 0 {
			content += key + "=" + value + "\n"
		}
	}

	writer := bufio.NewWriter(file)
	if _, err := writer.WriteString(content); err != nil {
		return err
	}

	return writer.Flush()
}

func (p *PersistenceImpl) AppendKeyValuePairsMap(data map[string]string) error {
	if p.dataFileName == "" {
		return ErrFileNameIsEmpty
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

func (p *PersistenceImpl) ReadKeyValuePairsToArray() ([]string, error) {

	if p.dataFileName == "" {
		return nil, ErrFileNameIsEmpty
	}

	data := []string{}
	path := p.dataFolderName + p.dataFileName

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrEmptyData
		}
		return nil, err
	}

	defer file.Close()

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
			data = append(data, key, strings.TrimSuffix(value, "\n"))
		}
	}

	return data, nil
}

func (p *PersistenceImpl) ReadAllFromFile() (map[string]string, error) {
	if p.dataFileName == "" {
		return nil, ErrFileNameIsEmpty
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

func (p *PersistenceImpl) ReadKeyValuePairsToMap(keys []string) (map[string]string, error) {
	if p.dataFileName == "" {
		return nil, ErrFileNameIsEmpty
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
