package logic

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"strconv"
)

func (n *RaftBrainImpl) serialize1() map[string]string {
	data := make(map[string]string)
	data["current_term"] = strconv.FormatInt(int64(n.currentTerm), 10)
	data["voted_for"] = strconv.FormatInt(int64(n.votedFor), 10)
	for index, log := range n.logs {
		key := fmt.Sprintf("log_%d", index)
		data[key] = log.ToString()
	}
	return data
}

func (n *RaftBrainImpl) serializeToArray() []string {
	data := []string{}
	data = append(data, "current_term", strconv.FormatInt(int64(n.currentTerm), 10))
	data = append(data, "voted_for", strconv.FormatInt(int64(n.votedFor), 10))
	for _, log := range n.logs {
		data = append(data, "append_log", log.ToString())
	}
	return data
}

func (n *RaftBrainImpl) deserializeFromArray(ctx context.Context, keyValuePairs []string) error {
	n.logs = []common.Log{}
	for i := 0; i < len(keyValuePairs); i += 2 {
		key, value := keyValuePairs[i], keyValuePairs[i+1]
		switch key {
		case "current_term":
			currentTerm, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			n.currentTerm = int(currentTerm)
		case "voted_for":
			votedFor, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return err
			}
			n.votedFor = int(votedFor)
		case "append_log":
			logItem, err := common.NewLogFromString(value)
			if err != nil {
				n.log().ErrorContext(ctx, "can not create log from string", err)

				return err
			} else {
				n.logs = append(n.logs, logItem)
			}
		case "delete_log":
			deletedLogCount, err := strconv.Atoi(value)
			if err != nil {
				return err
			} else {
				length := len(n.logs)
				n.logs = n.logs[:length-deletedLogCount]
			}
		}
	}

	return nil
}

func (n *RaftBrainImpl) deserialize1(ctx context.Context, data map[string]string) error {
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

	logCount := len(data) - 2

	n.logs = []common.Log{}
	for i := 0; i < int(logCount); i++ {
		key := fmt.Sprintf("log_%d", i)

		if value, ok := data[key]; ok {
			if value == TOMBSTONE {
				// Remove the last item
				n.logs = n.logs[:len(n.logs)-1]
			} else {
				logItem, err := common.NewLogFromString(value)
				if err != nil {
					n.log().ErrorContext(ctx, "can not create log from string", err)

					return err
				} else {
					n.logs = append(n.logs, logItem)
				}
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
	// keys, err := n.getPersistanceKeyList()
	// if err != nil {
	// 	if errors.Is(err, common.ErrEmptyData) {
	// 		n.log().ErrorContext(ctx, "data file is empty", err)
	// 		return nil
	// 	}

	// 	return err
	// }

	// data, err := n.db.ReadNewestLog(keys)
	// if err != nil {
	// 	return err
	// }

	data, err := n.db.ReadLogsToArray()
	if err != nil {
		if errors.Is(err, common.ErrEmptyData) {
			n.log().ErrorContext(ctx, "data file is empty", err)
			return nil
		}

		return err
	}

	if err := n.deserializeFromArray(ctx, data); err != nil {
		return err
	}

	return nil
}
