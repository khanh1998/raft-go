package logic

import (
	"khanh/raft-go/common"
)

func (r *RaftBrainImpl) StartSnapshot(snapshotFileName string) (res common.BeginSnapshotResponse, err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	if err = r.db.AppendKeyValuePairsArray("snapshot-start", snapshotFileName); err != nil {
		return res, err
	}

	index, term := r.lastLogInfo()

	return common.BeginSnapshotResponse{LastLogIndex: index, LastLogTerm: term}, nil
}

func (r *RaftBrainImpl) FinishSnapshot(metadata common.SnapshotMetadata) (err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	r.db.AppendKeyValuePairsArray("snapshot", metadata.ToString())
	if err != nil {
		return err
	}

	r.snapshot = &metadata

	return nil
}
