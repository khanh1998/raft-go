package logic

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
)

func (n *RaftBrainImpl) toCandidate() {
	n.log().Info().Msg("to candidate")
	n.State = common.StateCandidate
}

func (n *RaftBrainImpl) toLeader() {
	n.log().Info().Msg("to leader")
	n.State = common.StateLeader
	n.LeaderID = n.ID

	n.NextIndex = make(map[int]int)
	n.MatchIndex = make(map[int]int)

	for _, peer := range n.Members {
		if peer.ID != n.ID {
			n.NextIndex[peer.ID] = len(n.Logs) + 1
			n.MatchIndex[peer.ID] = 0
		}
	}

	n.appendLog(common.Log{
		Term:    n.CurrentTerm,
		Command: common.NoOperation,
	})
}

func (n *RaftBrainImpl) toFollower() {
	if n.State != common.StateCatchingUp {
		n.State = common.StateFollower
		n.log().Info().Msg("toFollower")
	} else {
		// if the current node's status is catching-up, i can't call this function to become a follower by itself,
		// need to wait the permission from the current leader.
		n.log().Error().Err(errors.New("catching-up can't be changed to follower with this function")).Msg("toFollower")
	}
}

// this will transition the node from non-voting to voting member (follower),
// trigger the election timeout and heartbeat timeout timer.
func (n *RaftBrainImpl) ToVotingMember() error {
	n.InOutLock.Lock()
	defer n.InOutLock.Unlock()

	if n.State == common.StateCatchingUp {
		n.State = common.StateFollower
		n.Start()
	} else {
		return fmt.Errorf("current status is %s", n.State)
	}
	return nil
}
