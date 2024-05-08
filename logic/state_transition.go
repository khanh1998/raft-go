package logic

import "khanh/raft-go/common"

func (n *RaftBrainImpl) toCandidate() {
	n.log().Info().Msg("to candidate")
	n.State = common.StateCandidate
}

func (n *RaftBrainImpl) toLeader() {
	n.log().Info().Msg("to leader")
	n.State = common.StateLeader

	n.NextIndex = make(map[int]int)
	n.MatchIndex = make(map[int]int)

	for _, peer := range n.Peers {
		n.NextIndex[peer.ID] = len(n.Logs) + 1
		n.MatchIndex[peer.ID] = 0
	}

	n.appendLog(common.Log{
		Term:    n.CurrentTerm,
		Command: common.NoOperation,
	})
}

func (n *RaftBrainImpl) toFollower() {
	n.log().Info().Msg("to follower")
	n.State = common.StateFollower
}
