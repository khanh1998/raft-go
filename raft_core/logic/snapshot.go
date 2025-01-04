package logic

import (
	"khanh/raft-go/raft_core/common"
)

func (r *RaftBrainImpl) StartSnapshot(snapshotFileName string) (res common.BeginSnapshotResponse, err error) {
	r.inOutLock.Lock()
	defer r.inOutLock.Unlock()

	index, term := r.lastLogInfo()

	return common.BeginSnapshotResponse{LastLogIndex: index, LastLogTerm: term}, nil
}
