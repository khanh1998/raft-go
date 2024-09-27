package logic

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
)

func (n *RaftBrainImpl) toCandidate(ctx context.Context) {
	n.log(ctx).Info().Msg("to candidate")
	n.state = common.StateCandidate
}

func (n *RaftBrainImpl) toLeader(ctx context.Context) {
	n.log(ctx).Info().Msg("to leader")
	n.state = common.StateLeader
	n.leaderID = n.id

	n.nextIndex = make(map[int]int)
	n.matchIndex = make(map[int]int)

	for _, peer := range n.members {
		if peer.ID != n.id {
			n.nextIndex[peer.ID] = len(n.logs) + 1
			n.matchIndex[peer.ID] = 0
		}
	}

	n.appendLog(ctx, common.Log{
		Term:    n.currentTerm,
		Command: common.NoOperation,
	})
}

func (n *RaftBrainImpl) toFollower(ctx context.Context) {
	if n.state != common.StateCatchingUp {
		n.state = common.StateFollower
		n.log(ctx).Info().Msg("toFollower")
	} else {
		// if the current node's status is catching-up, i can't call this function to become a follower by itself,
		// need to wait the permission from the current leader.
		n.log(ctx).Error().Err(errors.New("catching-up can't be changed to follower with this function")).Msg("toFollower")
	}
}

// this will transition the node from non-voting to voting member (follower),
// trigger the election timeout and heartbeat timeout timer.
func (n *RaftBrainImpl) ToVotingMember(ctx context.Context) error {
	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	n.log(ctx).Info().Msg("ToVotingMember")

	if n.state == common.StateCatchingUp {
		n.state = common.StateFollower
		n.Start()
	} else {
		return fmt.Errorf("current status is %s", n.state)
	}
	return nil
}
