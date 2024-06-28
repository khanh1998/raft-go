package logic

import (
	"khanh/raft-go/common"
)

// state machine will call this after it finish persisting a snapshot
func (n *RaftBrainImpl) NotifyNewSnapshot(noti common.SnapshotNotification) error {
	panic("unimplemented")
}

// leader will invoke this method to install snapshot on slow followers.
func (n *RaftBrainImpl) InstallSnapshot(input *common.InstallSnapshotInput, output *common.InstallSnapshotOutput) {
	panic("unimplemented")
}

func (n *RaftBrainImpl) SetStateMachine(sm SimpleStateMachine) {
	n.stateMachine = sm
}
