package common

import (
	"sort"
	"strings"
)

// this Persistence should be shared among goroutine
type PersistenceMockImpl struct {
	fileNames []string
	data      []string
}

func NewPersistenceMock() *PersistenceMockImpl {
	return &PersistenceMockImpl{
		data:      []string{},
		fileNames: []string{},
	}
}

func (p *PersistenceMockImpl) CreateNewFile(fileName string) error {
	p.data = []string{}
	return nil
}

func (p *PersistenceMockImpl) GetFileNames() ([]string, error) {
	return p.fileNames, nil
}

func (p *PersistenceMockImpl) OpenFile(fileName string) error {
	return nil
}

func (p PersistenceMockImpl) Data() []string {
	return p.data
}

func (p *PersistenceMockImpl) SetFileNames(fileNames []string) {
	p.fileNames = fileNames
}

func (p *PersistenceMockImpl) SetData(data []string) {
	p.data = data
}

func (p *PersistenceMockImpl) AppendLog(data map[string]string) error {
	sortedKeys := make([]string, 0, len(data))
	for k := range data {
		sortedKeys = append(sortedKeys, k)
	}

	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		if len(key) > 0 {
			content := key + "=" + data[key] + "\n"
			p.data = append(p.data, content)
		}
	}

	return nil
}

func (p *PersistenceMockImpl) ReadNewestLog(keys []string) (map[string]string, error) {
	data := make(map[string]string)
	if len(p.data) == 0 {
		return data, ErrEmptyData
	}

	sort.Strings(keys)
	for _, key := range keys {
		data[key] = ""
	}

	i := 0
	for {
		if i >= len(p.data) {
			break
		}

		line := p.data[i]

		tokens := strings.Split(line, "=")
		if len(tokens) == 2 {
			key, value := tokens[0], tokens[1]
			if _, ok := data[key]; ok {
				data[key] = strings.TrimSuffix(value, "\n")
			}
		}

		i += 1
	}

	return data, nil
}
