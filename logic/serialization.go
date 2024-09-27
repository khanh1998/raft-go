package logic

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"strconv"
	"time"
)

func (n *RaftBrainImpl) serialize(delimiter bool, createdAt bool, source string) map[string]string {
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

	data["current_term"] = strconv.FormatInt(int64(n.currentTerm), 10)
	data["voted_for"] = strconv.FormatInt(int64(n.votedFor), 10)
	data["log_count"] = strconv.FormatInt(int64(len(n.logs)), 10)
	for index, log := range n.logs {
		key := fmt.Sprintf("log_%d", index)
		data[key] = log.ToString()
	}
	return data
}

func (n *RaftBrainImpl) deserialize(ctx context.Context, data map[string]string) error {
	currentTerm, err := strconv.ParseInt(data["current_term"], 10, 32)
	if err != nil {
		return err
	}

	n.currentTerm = int(currentTerm)
	votedFor, err := strconv.ParseInt(data["voted_for"], 10, 32)
	if err != nil {
		return err
	}

	n.votedFor = int(votedFor)

	logCount, err := strconv.ParseInt(data["log_count"], 10, 32)
	if err != nil {
		return err
	}

	n.logs = []common.Log{}
	for i := 0; i < int(logCount); i++ {
		key := fmt.Sprintf("log_%d", i)

		if value, ok := data[key]; ok {
			logItem, err := common.NewLogFromString(value)
			if err != nil {
				n.log(ctx).Err(err).Msg("can not create log from string")

				return err
			} else {
				n.logs = append(n.logs, logItem)
			}
		} else {
			return errors.New("missing value to deserialize value of node")
		}
	}

	return nil
}

func (n *RaftBrainImpl) getPersistanceKeyList() ([]string, error) {
	data, err := n.db.ReadNewestLog([]string{"log_count"})
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

func (n *RaftBrainImpl) restoreRaftStateFromFile(ctx context.Context) error {
	keys, err := n.getPersistanceKeyList()
	if err != nil {
		if errors.Is(err, common.ErrEmptyData) {
			n.log(ctx).Err(err).Msg("data file is empty")
			return nil
		}

		return err
	}

	data, err := n.db.ReadNewestLog(keys)
	if err != nil {
		return err
	}

	if err := n.deserialize(ctx, data); err != nil {
		return err
	}

	return nil
}
