package logic

import (
	"context"
	"khanh/raft-go/common"
	"time"
)

// leader will invoke this method to install snapshot on slow followers.
func (n *RaftBrainImpl) InstallSnapshot(ctx context.Context, input *common.InstallSnapshotInput, output *common.InstallSnapshotOutput) {
	ctx, span := tracer.Start(ctx, "InstalSnapshot")
	defer span.End()

	defer func() {
		n.log().InfoContext(
			ctx, "InstallSnapshot",
			"input", *input,
			"output", *output,
		)
	}()

	// 1. reply immediately if term < currentTerm
	if input.Term < n.GetCurrentTerm() {
		output = &common.InstallSnapshotOutput{
			Term:    n.GetCurrentTerm(),
			Success: false,
			Message: "sender's term is smaller",
			NodeID:  n.id,
		}
		return
	}

	n.lastHeartbeatReceivedTime = time.Now()
	n.resetElectionTimeout(ctx)

	// 2. create snapshot file if first chunk (offset is 0)
	// 3. write data into snapshot file at given offset
	err := n.persistState.InstallSnapshot(ctx, input.FileName, input.Offset, input.Data)
	if err != nil {
		n.log().ErrorContext(ctx, "InstallSnapshot_InstallSnapshot", err)
		output = &common.InstallSnapshotOutput{
			Term:    n.GetCurrentTerm(),
			Success: false,
			Message: "failed to install snapshot part: " + err.Error(),
			NodeID:  n.id,
		}
		return
	}

	// 4. reply and wait for more data if done is false
	if !input.Done {
		output = &common.InstallSnapshotOutput{
			Term:    n.GetCurrentTerm(),
			Success: true,
			Message: "snapshot part was installed",
			NodeID:  n.id,
		}
		return
	}

	// 5. if lastIndex is greater than the latest snapshot's,
	// save snapshot file and Raft state(lastIndex, lastTerm, lastConfig),
	// discard any existing or partial snapshot.

	// todo: discard existing or partial snapshots
	latest := n.persistState.GetLatestSnapshotMetadata()
	if input.LastIndex <= latest.LastLogIndex {
		output = &common.InstallSnapshotOutput{
			Term:    n.GetCurrentTerm(),
			Success: true,
			Message: "lastIndex <= the latest snapshot's of responder",
			NodeID:  n.id,
		}
	}

	sm := common.SnapshotMetadata{
		LastLogTerm:  input.LastTerm,
		LastLogIndex: input.LastIndex,
		FileName:     input.FileName,
	}

	err = n.persistState.CommitSnapshot(ctx, sm)
	if err != nil {
		n.log().ErrorContext(ctx, "InstallSnapshot_CommitSnapshot", err)
		output = &common.InstallSnapshotOutput{
			Term:    n.GetCurrentTerm(),
			Success: false,
			Message: "failed to commit the snapshot: " + err.Error(),
			NodeID:  n.id,
		}
	}

	// 6. if existing log entry has index and term as same as lastIndex and lastTerm,
	// discard log up through lastIndex (but retain any following entries) and reply

	// 7. Discard the entire log (if any)
	n.persistState.DeleteAllLog(ctx)

	output = &common.InstallSnapshotOutput{
		Term:    n.GetCurrentTerm(),
		Success: true,
		Message: "full snapshot were installed successfully",
		NodeID:  n.id,
	}
	// 8. Reset state machine using snapshot contents
	// (and load lastConfig as cluster configuration)
	err = n.stateMachine.Reset(ctx)
	if err != nil {
		n.log().ErrorContext(ctx, "InstallSnapshot_Reset", err)
	}

	// load cluster config
	members := n.stateMachine.GetMembers()
	for _, mem := range members {
		err = n.addMember(mem.ID, mem.HttpUrl, mem.RpcUrl)
		if err != nil {
			n.log().ErrorContext(ctx, "InstallSnapshot_AddMember", err)
		}
	}

}

func (n *RaftBrainImpl) SetStateMachine(sm SimpleStateMachine) {
	n.stateMachine = sm
}
