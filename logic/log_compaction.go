package logic

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"time"
)

func (n *RaftBrainImpl) NextInstallSnapshotInput(ctx context.Context, peerId int, nextIdx int) (in common.InstallSnapshotInput, err error) {
	sm := n.persistState.GetLatestSnapshotMetadata()

	// sanity check
	if nextIdx > sm.LastLogIndex {
		return in, errors.New("snapshot's last index doesn't match with nextIndex")
	}

	offset := n.nextOffset[peerId]
	data, eof, err := n.persistState.StreamSnapshot(ctx, sm, offset.Offset, 100)
	if err != nil {
		return in, err
	}

	return common.InstallSnapshotInput{
		Term:       n.GetCurrentTerm(),
		LeaderId:   n.leaderID,
		LastIndex:  sm.LastLogIndex,
		LastTerm:   sm.LastLogTerm,
		LastConfig: n.members,
		FileName:   offset.FileName,
		Offset:     offset.Offset,
		Data:       data,
		Done:       eof,
	}, nil
}

// leader will invoke this method to install snapshot on slow followers.
func (n *RaftBrainImpl) SendInstallSnapshot(ctx context.Context, peerID int) (sm common.SnapshotMetadata, err error) {
	// todo: prevent leader taking new snapshot while installing snapshot to others
	ctx, span := tracer.Start(ctx, "SendInstalSnapshot")
	defer span.End()

	sm = n.persistState.GetLatestSnapshotMetadata()

	currentTerm := n.GetCurrentTerm()
	offset := int64(0)
	bufferedSize := 100                      // bytes
	fileName := common.NewSnapshotFileName() // please think about this
	for {
		data, eof, err := n.persistState.StreamSnapshot(ctx, sm, offset, bufferedSize)
		if err != nil {
			n.log().ErrorContext(ctx, "SendInstallSnapshot_StreamLatestSnapshot", err)

			return sm, err
		}

		// call rpc
		input := common.InstallSnapshotInput{
			Term:       currentTerm,
			LeaderId:   n.leaderID,
			LastTerm:   sm.LastLogTerm,
			LastIndex:  sm.LastLogIndex,
			LastConfig: []common.ClusterMember{},
			Offset:     offset,
			Data:       data,
			Done:       eof,
			FileName:   fileName,
		}

		_, err = n.rpcProxy.SendInstallSnapshot(ctx, peerID, &n.RpcRequestTimeout, input)
		if err != nil {
			return sm, err
		}

		offset += int64(len(data))

		if eof {
			break
		}
	}

	return sm, nil
}

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
