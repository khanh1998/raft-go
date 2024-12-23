package persistence_state

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/observability"
	"khanh/raft-go/raft_core/common"
	"sort"
)

type WalReader interface {
	WalIterator() func() (data []string, fileName string, err error)
	ReadObject(fileName string, result common.DeserializableObject) error
	GetObjectNames() ([]string, error)
}

func findLatestSnapshot(fileNames []string) (fileName string, err error) {
	var snapshotFiles []string

	for _, file := range fileNames {
		if gc.IsSnapshotFile(file) {
			snapshotFiles = append(snapshotFiles, file)
		}
	}

	if len(snapshotFiles) > 0 {
		sort.Strings(snapshotFiles)
		last := len(snapshotFiles) - 1
		fileName = snapshotFiles[last]
	}

	return fileName, nil
}

func Deserialize(ctx context.Context, s WalReader, clusterMode gc.ClusterMode, logger observability.Logger, logFactory gc.LogFactory) (latestSnapshot gc.Snapshot, raftPersistedState *RaftPersistenceStateImpl, clusterMembers []gc.ClusterMember, err error) {
	latestSnapshot = logFactory.EmptySnapshot()
	raftPersistedState = &RaftPersistenceStateImpl{
		votedFor:    0,
		currentTerm: 0,
		logs:        []gc.Log{},
		logger:      logger,
		logFactory:  logFactory,
	}

	fileNames, err := s.GetObjectNames()
	if err != nil {
		return nil, nil, nil, err
	}

	snapshotFileName, err := findLatestSnapshot(fileNames)
	if err != nil {
		return nil, nil, nil, err
	}

	if snapshotFileName != "" {
		err = s.ReadObject(snapshotFileName, latestSnapshot)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	walLastLogIndex := []int{}
	nextWal := s.WalIterator()

	for {
		data, _, err := nextWal()
		if err != nil {
			return nil, nil, nil, err
		}

		if data == nil {
			break
		}

		lastLogIndex, err := raftPersistedState.Deserialize(data, latestSnapshot.Metadata())
		if err != nil {
			return nil, nil, nil, err
		}

		// if the current WAL contains no log,
		// take the last log index from the previous WAL
		if lastLogIndex == 0 && len(walLastLogIndex) > 0 {
			lastLogIndex = walLastLogIndex[len(walLastLogIndex)-1]
		}
		walLastLogIndex = append(walLastLogIndex, lastLogIndex)
	}
	sm := latestSnapshot.Metadata()

	raftPersistedState.latestSnapshot = sm

	// rebuild the cluster member configuration from logs and snapshot for dynamic cluster,
	// for static cluster, member configurations are read from config file.
	if clusterMode == gc.Dynamic {
		clusterMemberMap := map[int]gc.ClusterMember{}
		lastConfig := latestSnapshot.GetLastConfig()
		if len(lastConfig) > 0 {
			for peerId, peer := range lastConfig {
				clusterMemberMap[peerId] = peer
			}
		}

		for _, logs := range raftPersistedState.logs {
			addition, peerId, httpUrl, rpcUrl, err := logs.DecomposeChangeSeverCommand()
			if err != nil {
				continue
			}

			if addition {
				clusterMemberMap[peerId] = gc.ClusterMember{
					ID:      peerId,
					RpcUrl:  rpcUrl,
					HttpUrl: httpUrl,
				}
			}

			if !addition {
				delete(clusterMemberMap, peerId)
			}
		}

		for _, peer := range clusterMemberMap {
			clusterMembers = append(clusterMembers, peer)
		}
	}

	// finding the newest WAL that older than the latest snapshot,
	// then delete that WAL and all previous WALs.
	toBeDeletedWal := ""
	for i := 0; i < len(walLastLogIndex); i++ {
		if walLastLogIndex[i] < sm.LastLogIndex {
			toBeDeletedWal = "" // todo
		} else {
			break
		}
	}

	_ = toBeDeletedWal

	// cleanup outdated WALs and snapshots
	// delete outdated WALs
	// delete outdated snapshots

	return latestSnapshot, raftPersistedState, clusterMembers, nil
}
