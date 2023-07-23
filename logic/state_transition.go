package logic

func (n *RaftBrainImpl) ToCandidate() {
	n.log().Info().Msg("to candidate")
	n.State = StateCandidate
}

func (n *RaftBrainImpl) ToLeader() {
	n.log().Info().Msg("to leader")
	n.State = StateLeader

	n.NextIndex = make(map[int]int)
	n.MatchIndex = make(map[int]int)

	for _, peer := range n.Peers {
		n.NextIndex[peer.ID] = len(n.Logs) + 1
		n.MatchIndex[peer.ID] = 0
	}
}

func (n *RaftBrainImpl) ToFollower() {
	n.log().Info().Msg("to follower")
	n.State = StateFollower
}
