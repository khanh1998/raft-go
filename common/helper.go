package common

import (
	"context"
	"sort"
)

type WalReader interface {
	ReadAllWal() (data [][]string, err error)
	ReadObject(fileName string, result DeserializableObject) error
	GetFileNames() ([]string, error)
}

func findLatestSnapshot(fileNames []string) (fileName string, err error) {
	var snapshotFiles []string

	for _, file := range fileNames {
		if IsSnapshotFile(file) {
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

func Deserialize(ctx context.Context, s WalReader, clusterMode ClusterMode) (latestSnapshot *Snapshot, raftPersistedState *RaftPersistanceStateImpl, clusterMembers []ClusterMember, err error) {
	latestSnapshot = NewSnapshot()
	raftPersistedState = &RaftPersistanceStateImpl{
		votedFor:    0,
		currentTerm: 0,
		logs:        []Log{},
	}

	fileNames, err := s.GetFileNames()
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

	walData, err := s.ReadAllWal()
	if err != nil && err != ErrEmptyData {
		return nil, nil, nil, err
	}

	walLastLogIndex := make([]int, len(walData))

	for i := 0; i < len(walData); i++ {
		lastLogIndex, err := raftPersistedState.Deserialize(walData[i], latestSnapshot.Metadata())
		if err != nil {
			return nil, nil, nil, err
		}

		// if the current WAL contains no log,
		// take the last log index from the previous WAL
		if lastLogIndex == 0 {
			lastLogIndex = walLastLogIndex[i-1]
		}
		walLastLogIndex[i] = lastLogIndex
	}

	// rebuild the cluster member configuration from logs and snapshot for dynamic cluster,
	// for static cluster, member configurations are read from config file.
	if clusterMode == Dynamic {
		clusterMemberMap := map[int]ClusterMember{}
		if len(latestSnapshot.LastConfig) > 0 {
			for peerId, peer := range latestSnapshot.LastConfig {
				clusterMemberMap[peerId] = peer
			}
		}

		for _, logs := range raftPersistedState.logs {
			addition, peerId, httpUrl, rpcUrl, err := DecomposeChangeSeverCommand(logs.Command)
			if err != nil {
				continue
			}

			if addition {
				clusterMemberMap[peerId] = ClusterMember{
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
		if walLastLogIndex[i] < latestSnapshot.LastLogIndex {
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
