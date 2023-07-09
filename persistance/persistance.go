package persistance

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
)

type Persistence interface {
	AppendLog(data map[string]string) error
	ReadNewestLog(keys []string) (map[string]string, error)
}

// this Persistence should be shared among goroutine
type PersistenceImpl struct {
	dataFileName string
}

func NewPersistence(fileName string) Persistence {
	return &PersistenceImpl{
		dataFileName: fileName,
	}
}

func (p *PersistenceImpl) AppendLog(data map[string]string) error {
	file, err := os.OpenFile(p.dataFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

var ErrEmptyData = errors.New("empty data")

func (p *PersistenceImpl) ReadNewestLog(keys []string) (map[string]string, error) {
	data := make(map[string]string)

	file, err := os.OpenFile(p.dataFileName, os.O_RDONLY, 0644)
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
