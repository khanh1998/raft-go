package logic

import "khanh/raft-go/common"

func (r *RaftBrainImpl) GetId() int {
	return r.id
}

func (r *RaftBrainImpl) GetLatestSnapshotMetadata() common.SnapshotMetadata {
	return r.persistState.GetLatestSnapshotMetadata()
}

func (r *RaftBrainImpl) GetCurrentTerm() int {
	if r.persistState == nil {
		return 0
	}
	return r.persistState.GetCurrentTerm()
}

func (r *RaftBrainImpl) GetVotedFor() int {
	if r.persistState == nil {
		return 0
	}
	return r.persistState.GetVotedFor()
}

func (r *RaftBrainImpl) GetLogLength() int {
	if r.persistState == nil {
		return 0
	}
	return r.persistState.LogLength()
}

func (r *RaftBrainImpl) GetState() common.RaftState {
	return r.state
}

func (n *RaftBrainImpl) GetInfo() common.GetStatusResponse {
	return common.GetStatusResponse{
		ID:          n.id,
		State:       n.state,
		Term:        n.GetCurrentTerm(),
		LeaderId:    n.leaderID,
		ClusterTime: n.clusterClock.clusterTimeAtEpoch,
	}
}
