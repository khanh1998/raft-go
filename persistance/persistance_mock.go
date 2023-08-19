package persistance

import (
	"sort"
	"strings"
)

type PersistenceMock interface {
	AppendLog(data map[string]string) error
	ReadNewestLog(keys []string) (map[string]string, error)
	Data() []string
	SetData([]string)
}

// this Persistence should be shared among goroutine
type PersistenceMockImpl struct {
	data []string
}

func NewPersistenceMock() PersistenceMock {
	return &PersistenceMockImpl{
		data: []string{},
	}
}

func (p PersistenceMockImpl) Data() []string {
	return p.data
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
