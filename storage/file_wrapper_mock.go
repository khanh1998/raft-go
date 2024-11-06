package storage

import (
	"errors"
	"strings"
)

type FileWrapperMock struct {
	Data map[string][]string
	Size map[string]int64
}

func NewFileWrapperMock() FileWrapperMock {
	return FileWrapperMock{Data: make(map[string][]string), Size: make(map[string]int64)}
}

func getSize(data []string) (size int64) {
	for _, s := range data {
		size += int64(len(s))
	}

	return size
}

func (f FileWrapperMock) GetFileNames(folder string) (names []string, err error) {
	for path, _ := range f.Data {
		if strings.HasPrefix(path, folder) {
			tokens := strings.Split(path, "/")
			fileName := tokens[len(tokens)-1]
			names = append(names, fileName)
		}
	}

	return names, nil
}

func (f FileWrapperMock) ReadStrings(path string) ([]string, int64, error) {
	if value, ok := f.Data[path]; ok {
		return value, f.Size[path], nil
	}

	return nil, 0, errors.New("path does not exist")
}

func (f FileWrapperMock) AppendStrings(path string, lines []string) (fileSize int64, err error) {
	value, ok := f.Data[path]
	if ok {
		f.Data[path] = append(value, lines...)
		f.Size[path] += getSize(lines)
	} else {
		f.Data[path] = lines
		f.Size[path] = getSize(lines)
	}

	return f.Size[path], nil
}

func (f FileWrapperMock) AppendKeyValuePairs(path string, keyValues ...string) (int64, error) {
	length := len(keyValues)
	lines := []string{}
	for i := 0; i < length; i += 2 {
		key, value := keyValues[i], keyValues[i+1]
		if len(key) > 0 {
			line := key + "=" + value
			lines = append(lines, line)
		}
	}

	value, ok := f.Data[path]
	if ok {
		f.Data[path] = append(value, lines...)
		f.Size[path] += getSize(lines)
	} else {
		f.Data[path] = lines
		f.Size[path] = getSize(lines)
	}

	return f.Size[path], nil
}

func (f FileWrapperMock) ReadKeyValuePairsToArray(path string) ([]string, int64, error) {
	lines, ok := f.Data[path]
	if !ok {
		return nil, 0, errors.New("path does not exist")
	}

	keyValuePairs := []string{}
	for _, line := range lines {
		tokens := strings.Split(line, "=")
		key, value := tokens[0], tokens[1]
		keyValuePairs = append(keyValuePairs, key, value)
	}

	return keyValuePairs, f.Size[path], nil
}

func (f FileWrapperMock) DeleteFile(path string) error {
	if _, ok := f.Data[path]; !ok {
		return errors.New("path does not exist")
	}

	delete(f.Data, path)
	delete(f.Size, path)

	return nil
}

func (f FileWrapperMock) Rename(oldPath string, newPath string) (err error) {
	value, ok := f.Data[oldPath]
	if !ok {
		return errors.New("old file doesn't exist")
	}

	_, ok = f.Data[newPath]
	if ok {
		return errors.New("new file does exist")
	}

	f.Data[newPath] = value
	f.Size[newPath] = f.Size[oldPath]
	delete(f.Data, oldPath)
	delete(f.Size, oldPath)

	return nil
}
