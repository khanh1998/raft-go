package logic

import "khanh/raft-go/common"

func (r *RaftBrainImpl) GetId() int {
	return r.id
}

func (r *RaftBrainImpl) GetCurrentTerm() int {
	return r.currentTerm
}

func (r *RaftBrainImpl) GetState() common.RaftState {
	return r.state
}

func (n *RaftBrainImpl) GetInfo() common.GetStatusResponse {
	return common.GetStatusResponse{
		ID:       n.id,
		State:    n.state,
		Term:     n.currentTerm,
		LeaderId: n.leaderID,
	}
}
