package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"khanh/raft-go/common"
	"os"
	"strings"
)

// this is just a wrapper on the built in file library,
// so that i can mock and test
type FileWrapperImpl struct{}

func (f FileWrapperImpl) GetFileNames(folder string) (names []string, err error) {
	entries, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			fileInfo, err := e.Info()
			if err != nil {
				return nil, err
			}

			fileName := fileInfo.Name()
			names = append(names, fileName)
		}
	}

	return names, nil
}

func (f FileWrapperImpl) ReadStrings(path string) ([]string, int64, error) {
	data := []string{}

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, common.ErrEmptyData
		}
		return nil, 0, err
	}

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return data, 0, err
		}

		data = append(data, strings.TrimSuffix(line, "\n"))
	}

	return data, stat.Size(), nil
}

func (f FileWrapperImpl) AppendStrings(path string, lines []string) (fileSize int64, err error) {
	if len(lines) == 0 {
		return 0, errors.New("input data is empty")
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	content := ""
	for _, line := range lines {
		content += line + "\n"
	}

	writer := bufio.NewWriter(file)
	contentSize, err := writer.WriteString(content)
	if err != nil {
		return 0, err
	}

	fileSize = stat.Size() + int64(contentSize)

	if err = writer.Flush(); err != nil {
		return 0, err
	}

	return fileSize, nil
}

func (f FileWrapperImpl) AppendKeyValuePairs(path string, keyValues ...string) (int64, error) {
	if len(keyValues) == 0 {
		return 0, errors.New("input data is empty")
	}

	length := len(keyValues)
	if length%2 != 0 {
		return 0, fmt.Errorf("length of input array must be even: %d", len(keyValues))
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	content := ""
	for i := 0; i < length; i += 2 {
		key, value := keyValues[i], keyValues[i+1]
		if len(key) > 0 {
			content += key + "=" + value + "\n"
		}
	}

	writer := bufio.NewWriter(file)
	contentSize, err := writer.WriteString(content)
	if err != nil {
		return 0, err
	}

	fileSize := stat.Size() + int64(contentSize)

	return fileSize, writer.Flush()
}

func (f FileWrapperImpl) ReadKeyValuePairsToArray(path string) ([]string, int64, error) {
	data := []string{}

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, common.ErrEmptyData
		}
		return nil, 0, err
	}

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return data, 0, err
		}

		tokens := strings.Split(line, "=")
		if len(tokens) == 2 {
			key, value := tokens[0], tokens[1]
			data = append(data, key, strings.TrimSuffix(value, "\n"))
		}
	}

	return data, stat.Size(), nil
}

func (f FileWrapperImpl) DeleteFile(path string) error {
	return os.Remove(path)
}

func (f FileWrapperImpl) Rename(oldPath string, newPath string) (err error) {
	return os.Rename(oldPath, newPath)
}
