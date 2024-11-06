package logic

import (
	"khanh/raft-go/common"
)

func (r *RaftBrainImpl) StartSnapshot(snapshotFileName string) (res common.BeginSnapshotResponse, err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	index, term := r.lastLogInfo()

	return common.BeginSnapshotResponse{LastLogIndex: index, LastLogTerm: term}, nil
}

func (r *RaftBrainImpl) deleteLogAfterSnapshot(metadata common.SnapshotMetadata) (err error) {

	return nil
}

func (r *RaftBrainImpl) FinishSnapshot(metadata common.SnapshotMetadata) (err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	return nil
}
