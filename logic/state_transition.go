package logic

func (n *NodeImpl) ToCandidate() {
	n.log().Info().Msg("to candidate")
	n.State = StateCandidate
}

func (n *NodeImpl) ToLeader() {
	n.log().Info().Msg("to leader")
	n.State = StateLeader

	n.NextIndex = make([]int, len(n.PeerURLs))
	n.MatchIndex = make([]int, len(n.PeerURLs))
	for i := 0; i < len(n.PeerURLs); i++ {
		n.NextIndex[i] = len(n.Logs) + 1
		n.MatchIndex[i] = 0
	}
}

func (n *NodeImpl) ToFollower() {
	n.log().Info().Msg("to follower")
	n.State = StateFollower
}
