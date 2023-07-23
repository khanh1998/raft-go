package logic

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"khanh/raft-go/persistance"
	"strconv"
	"time"
)

func (n RaftBrainImpl) Serialize(delimiter bool, createdAt bool, source string) map[string]string {
	data := make(map[string]string)
	if delimiter {
		data["a"] = "---------------------------------------------"
	}
	if len(source) > 0 {
		data["a source"] = source
	}
	if createdAt {
		data["time"] = time.Now().Format(time.RFC3339)
	}

	data["current_term"] = strconv.FormatInt(int64(n.CurrentTerm), 10)
	data["voted_for"] = strconv.FormatInt(int64(n.VotedFor), 10)
	data["log_count"] = strconv.FormatInt(int64(len(n.Logs)), 10)
	for index, log := range n.Logs {
		key := fmt.Sprintf("log_%d", index)
		data[key] = log.ToString()
	}
	return data
}

func (n *RaftBrainImpl) Deserialize(data map[string]string) error {
	currentTerm, err := strconv.ParseInt(data["current_term"], 10, 32)
	if err != nil {
		return err
	}

	n.CurrentTerm = int(currentTerm)
	votedFor, err := strconv.ParseInt(data["voted_for"], 10, 32)
	if err != nil {
		return err
	}

	n.VotedFor = int(votedFor)

	logCount, err := strconv.ParseInt(data["log_count"], 10, 32)
	if err != nil {
		return err
	}

	n.Logs = []common.Log{}
	for i := 0; i < int(logCount); i++ {
		key := fmt.Sprintf("log_%d", i)

		if value, ok := data[key]; ok {
			logItem, err := common.NewLogFromString(value)
			if err != nil {
				n.log().Err(err).Msg("can not create log from string")
			}

			n.Logs = append(n.Logs, logItem)
		} else {
			return errors.New("missing value to deserialize value of node")
		}
	}

	return nil
}

func (n *RaftBrainImpl) GetPersistanceKeyList() ([]string, error) {
	data, err := n.DB.ReadNewestLog([]string{"log_count"})
	if err != nil {
		return nil, err
	}

	var countStr string
	var ok bool
	if countStr, ok = data["log_count"]; !ok {
		return nil, errors.New("cannot retrieve log_count from persistent layer")
	}

	logCount, err := strconv.ParseInt(countStr, 10, 32)
	if err != nil {
		return nil, err
	}
	keys := []string{"current_term", "voted_for", "log_count"}

	for i := 0; i < int(logCount); i++ {
		keys = append(keys, fmt.Sprintf("log_%d", i))
	}

	return keys, nil
}

func (n *RaftBrainImpl) Rehydrate() error {
	n.log().Info().Msg("started to rehydrate")
	keys, err := n.GetPersistanceKeyList()
	if err != nil {
		if errors.Is(err, persistance.ErrEmptyData) {
			n.log().Err(err).Msg("data file is empty")
			return nil
		}

		return err
	}

	data, err := n.DB.ReadNewestLog(keys)
	if err != nil {
		return err
	}

	if err := n.Deserialize(data); err != nil {
		return err
	}

	return nil
}
