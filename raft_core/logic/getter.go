package logic

import (
	gc "khanh/raft-go/common"
)

func (r *RaftBrainImpl) GetId() int {
	return r.id
}

func (r *RaftBrainImpl) GetLatestSnapshotMetadata() gc.SnapshotMetadata {
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

func (r *RaftBrainImpl) GetState() gc.RaftState {
	return r.state
}

func (n *RaftBrainImpl) GetInfo() gc.GetStatusResponse {
	return gc.GetStatusResponse{
		ID:          n.id,
		State:       n.state,
		Term:        n.GetCurrentTerm(),
		LeaderId:    n.leaderID,
		ClusterTime: n.clusterClock.clusterTimeAtEpoch,
		CommitIndex: n.commitIndex,
	}
}

func (n *RaftBrainImpl) GetMembers() []gc.ClusterMember {
	return n.members
}
