package logic

import (
	gc "khanh/raft-go/common"
	"khanh/raft-go/raft_core/common"
)

func (r *RaftBrainImpl) StartSnapshot(snapshotFileName string) (res common.BeginSnapshotResponse, err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	index, term := r.lastLogInfo()

	return common.BeginSnapshotResponse{LastLogIndex: index, LastLogTerm: term}, nil
}

func (r *RaftBrainImpl) deleteLogAfterSnapshot(metadata gc.SnapshotMetadata) (err error) {

	return nil
}

func (r *RaftBrainImpl) FinishSnapshot(metadata gc.SnapshotMetadata) (err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	return nil
}
