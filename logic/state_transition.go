package logic

func (n *NodeImpl) ToCandidate() {
	n.log().Info().Msg("to candidate")
	n.State = StateCandidate
}

func (n *NodeImpl) ToLeader() {
	n.log().Info().Msg("to leader")
	n.State = StateLeader
}

func (n *NodeImpl) ToFollower() {
	n.log().Info().Msg("to follower")
	n.State = StateFollower
}
