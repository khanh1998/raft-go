package logic

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"strconv"
)

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
	sm := common.SnapshotMetadata{}
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
		case "snapshot":
			if err := sm.FromString(value); err != nil {
				return err
			}
			n.snapshot = &sm
		}

	}

	return nil
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

	// data, err := n.db.ReadKeyValuePairsToMap(keys)
	// if err != nil {
	// 	return err
	// }

	data, err := n.db.ReadKeyValuePairsToArray()
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
