package logic

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"os"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

func (r *RaftBrainImpl) isValidAddRequest(input common.Log) error {
	addition, id, httpUrl, rpcUrl, err := input.DecomposeChangeSeverCommand()
	if err != nil {
		return err
	}

	if !addition {
		return fmt.Errorf("wrong add server command: %v", input)
	}

	for _, mem := range r.members {
		if mem.ID == id || mem.HttpUrl == httpUrl || mem.RpcUrl == rpcUrl {
			return errors.New("the server's information is duplicated")
		}
	}
	if id < r.nextMemberId {
		return fmt.Errorf("server is should be %d or bigger than that", r.nextMemberId)
	}
	return nil
}

func (r *RaftBrainImpl) isValidRemoveRequest(input common.Log) (error, int) {
	addition, id, httpUrl, rpcUrl, err := input.DecomposeChangeSeverCommand()
	if err != nil {
		return err, 0
	}

	if addition {
		return fmt.Errorf("wrong remove server command: %v", input), 0
	}

	for _, mem := range r.members {
		if mem.ID == id && mem.HttpUrl == httpUrl && mem.RpcUrl == rpcUrl {
			return nil, id
		}
	}
	return errors.New("can't found the server to remove"), 0
}

func (r *RaftBrainImpl) WaitForLogCommitted(dur time.Duration, index int) error {
	timeout := time.After(dur)
	stop := false
	for !stop {
		select {
		case <-timeout:
			return errors.New("timeout: wait for log committed")
		default:
			if r.commitIndex >= index {
				stop = true
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func (r *RaftBrainImpl) RemoveServer(ctx context.Context, input common.Log, output *common.RemoveServerOutput) (err error) {
	ctx, span := tracer.Start(ctx, "RemoveServer")
	defer span.End()

	r.inOutLock.Lock()
	_ = output // to disable the warning: argument output is overwritten before first use

	isLeader := r.state == common.StateLeader
	if !isLeader {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   common.NotLeader,
		}

		r.inOutLock.Unlock()
		return nil
	}

	var removedId int
	if err, removedId = r.isValidRemoveRequest(input); err != nil {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.inOutLock.Unlock()
		return nil
	}

	r.inOutLock.Unlock()

	if !r.changeMemberLock.TryLock() {
		err = errors.New("the server requested to be removed from the cluster is not exist")

		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.changeMemberLock.Unlock()

	newLog, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	if err != nil {
		return err
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "RemoveServer_appendLog", err)

		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	msg := fmt.Sprintf("server %d can be shut down now", removedId)
	err = r.WaitForLogCommitted(30*time.Second, index)
	if err != nil {
		msg = fmt.Sprintf("log isn't committed, server %d can't be shut down", removedId)
	}

	isLeaderRemoved := isLeader && removedId == r.id
	if isLeaderRemoved {
		r.state = common.StateRemoved
		r.stop()
		r.log().InfoContext(ctx, "the server will be shut down in 5s")
		time.AfterFunc(5*time.Second, func() {
			pid := os.Getpid()
			syscall.Kill(pid, syscall.SIGTERM)
		})
	}

	*output = common.RemoveServerOutput{
		Status:     common.StatusOK,
		LeaderHint: "",
		Response:   msg,
	}

	return nil
}

func (r *RaftBrainImpl) AddServer(ctx context.Context, input common.Log, output *common.AddServerOutput) (err error) {
	ctx, span := tracer.Start(ctx, "AddServer")
	defer span.End()

	r.inOutLock.Lock()
	if r.state != common.StateLeader {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   common.NotLeader,
		}

		r.inOutLock.Unlock()
		return nil
	}

	if err := r.isValidAddRequest(input); err != nil {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.inOutLock.Unlock()
		return nil
	}

	newLog, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	if err != nil {
		return err
	}

	r.inOutLock.Unlock()

	if !r.changeMemberLock.TryLock() {
		err = errors.New("another node is adding (removing) to the cluster")

		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.changeMemberLock.Unlock()
	timeout := 5 * time.Second

	_, newServerId, _, newServerRpcUrl, _ := input.DecomposeChangeSeverCommand()

	if err := r.rpcProxy.ConnectToNewPeer(ctx, newServerId, newServerRpcUrl, 5, timeout); err != nil {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		log.Err(err).Msg("ConnectToNewPeer")

		return nil
	}

	// catch up new server for fixed number of rounds
	for i := 0; i < 10; i++ {
		log.Info().Msgf("AddServer catchup start round %d", i)
		begin := time.Now()

		err := r.catchUpWithNewMember(ctx, newServerId)
		if err != nil {
			*output = common.AddServerOutput{
				Status:     common.StatusNotOK,
				LeaderHint: r.getLeaderHttpUrl(),
				Response:   err.Error(),
			}

			return nil
		} else {
			duration := time.Since(begin)
			if duration < r.electionTimeOutMin {
				break
			}
		}
		log.Info().Msgf("AddServer catchup end round %d", i)
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "AddServer_appendLog", err)

		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	// after append new config to log, we use it immediately without waiting commit

	r.nextMemberId = newServerId + 1

	// allow new server to become follower
	err = r.rpcProxy.SendToVotingMember(ctx, newServerId, &timeout)
	if err != nil {
		log.Err(err).Msg("SendToVotingMember")
	}

	// wait for log to committed
	msg := ""
	err = r.WaitForLogCommitted(30*time.Second, index)
	if err != nil {
		msg = fmt.Sprintf("log isn't committed, server %d isn't surely joined the cluster", newServerId)
	}

	*output = common.AddServerOutput{
		Status:   common.StatusOK,
		Response: msg,
	}

	return nil
}

func (r *RaftBrainImpl) catchUpWithNewMember(ctx context.Context, peerID int) error {
	initNextIdx := r.persistState.LogLength()
	nextIdx := initNextIdx
	currentTerm := r.persistState.GetCurrentTerm()
	matchIndex := 0
	nextOffset := NextOffset{} // empty

	for matchIndex < initNextIdx {

		input := common.AppendEntriesInput{
			Term:         currentTerm,
			LeaderID:     r.id,
			LeaderCommit: r.commitIndex,
		}

		if nextIdx > 1 {
			input.PrevLogIndex = nextIdx - 1

			prevLog, err := r.GetLog(nextIdx - 1)
			if err == nil || errors.Is(err, common.ErrLogIsInSnapshot) {
				// if previous log is in snapshot, we just sent lastTerm and lastIndex
				input.PrevLogTerm = prevLog.GetTerm()
			}
		}

		logItem, err := r.GetLog(nextIdx)
		if err == nil {
			input.Entries = []common.Log{logItem}
		} else if errors.Is(err, common.ErrLogIsInSnapshot) {
			// the log that need to send to follower is compacted into snapshot,
			// so we need to install snapshot to follower
			if nextOffset == (NextOffset{}) {
				sm := r.GetLatestSnapshotMetadata()
				nextOffset = NextOffset{0, common.NewSnapshotFileName(sm.LastLogTerm, sm.LastLogIndex), sm}
			}
		} else {
			r.log().ErrorContext(ctx, "BroadcastAppendEntries_GetLog", err)
		}

		timeout := 5 * time.Second

		if nextOffset != (NextOffset{}) {
			// sanity check
			// check if there is a newer snapshot
			latestSnapshot := r.GetLatestSnapshotMetadata()
			if nextOffset.Snapshot != latestSnapshot {
				r.log().ErrorContext(
					ctx, "GetLatestSnapshotMetadata",
					errors.New("there is new snapshot"),
					"offset", nextOffset,
					"newSnapshot", latestSnapshot,
				)
				nextOffset = NextOffset{
					Offset:   0,
					FileName: common.NewSnapshotFileName(latestSnapshot.LastLogTerm, latestSnapshot.LastLogIndex),
					Snapshot: latestSnapshot} // reset the snapshot install progress
				break
			}

			data, eof, err := r.persistState.StreamSnapshot(ctx, nextOffset.Snapshot, nextOffset.Offset, r.snapshotChunkSize)
			if err != nil {
			}

			input := common.InstallSnapshotInput{
				Term:       r.GetCurrentTerm(),
				LeaderId:   r.leaderID,
				LastIndex:  nextOffset.Snapshot.LastLogIndex,
				LastTerm:   nextOffset.Snapshot.LastLogTerm,
				LastConfig: r.members,
				FileName:   nextOffset.FileName,
				Offset:     nextOffset.Offset,
				Data:       data,
				Done:       eof,
			}

			output, err := r.rpcProxy.SendInstallSnapshot(ctx, peerID, &timeout, input)
			if err != nil {
				r.log().ErrorContext(ctx, "BroadcastAppendEntries_SendInstallSnapshot", err)
			} else {
				// successCount += 1
			}

			_ = output

			if !input.Done {
				nextOffset = NextOffset{
					Offset:   nextOffset.Offset + int64(r.snapshotChunkSize),
					FileName: nextOffset.FileName,
					Snapshot: nextOffset.Snapshot,
				}
			}

			if input.Done {
				nextOffset = NextOffset{}
				matchIndex = input.LastIndex
				nextIdx = input.LastIndex + 1
			}
		} else {
			output, err := r.rpcProxy.SendAppendEntries(ctx, peerID, &timeout, input)
			r.log().DebugContext(ctx, "r.RpcProxy.SendAppendEntries", "output", output)
			if err != nil {
				r.log().ErrorContext(ctx, "r.rpcProxy.SendAppendEntries", err)
			} else {
				if output.Success && output.Term > currentTerm {
					r.log().WarnContext(ctx, "inconsistent response", "response", output)
				} else if output.Success {
					matchIndex = common.Min(nextIdx, initNextIdx)

					nextIdx = common.Min(nextIdx+1, initNextIdx+1)
				} else {
					if output.Term <= r.persistState.GetCurrentTerm() {
						nextIdx = common.Max(0, nextIdx-1)
					} else {
						// the appendEntries request is failed,
						// because current leader is outdated
						r.log().ErrorContext(ctx, "the follower cannot be more up to date than the current leader", nil)
						return err
					}
				}
			}
		}

	}

	r.log().InfoContext(ctx, "finish catch up", "matchIndex", matchIndex, "initNextIdx", initNextIdx)

	return nil
}

func (r *RaftBrainImpl) revertChangeMember(log common.Log) error {
	addition, id, httpUrl, rpcUrl, err := log.DecomposeChangeSeverCommand()
	if err != nil {
		return err
	}

	// revert change
	if addition {
		return r.removeMember(id, httpUrl, rpcUrl)
	} else {
		return r.addMember(id, httpUrl, rpcUrl)
	}
}

func (r *RaftBrainImpl) changeMember(log common.Log) error {
	defer func() {
		r.log().Info("changeMember", "command", log)
	}()
	addition, id, httpUrl, rpcUrl, err := log.DecomposeChangeSeverCommand()
	if err != nil {
		return err
	}

	if addition {
		return r.addMember(id, httpUrl, rpcUrl)
	} else {
		return r.removeMember(id, httpUrl, rpcUrl)
	}
}

func (r *RaftBrainImpl) removeMember(id int, httpUrl, rpcUrl string) error {
	defer func() {
		r.log().Info("removeMember", "id", id, "httpUrl", httpUrl, "rpcUrl", rpcUrl, "members", r.members)
	}()
	tmp := []common.ClusterMember{}
	for _, mem := range r.members {
		if mem.ID != id {
			tmp = append(tmp, mem)
		}
	}
	r.members = tmp

	if r.id != id {
		r.newMembers <- common.ClusterMemberChange{
			ClusterMember: common.ClusterMember{
				ID:      id,
				RpcUrl:  rpcUrl,
				HttpUrl: httpUrl,
			},
			Add: false,
		}

		if r.state == common.StateLeader {
			delete(r.nextIndex, id)
			delete(r.matchIndex, id)
		}
	}

	return nil
}

func (r *RaftBrainImpl) addMember(id int, httpUrl, rpcUrl string) error {
	defer func() {
		r.log().Info("addMember", "id", id, "httpUrl", httpUrl, "rpcUrl", rpcUrl, "members", r.members)
	}()
	for _, mem := range r.members {
		if mem.ID == id {
			return fmt.Errorf("duplicated cluster member %d %s %s", id, httpUrl, rpcUrl)
		}
	}

	r.members = append(r.members, common.ClusterMember{
		HttpUrl: httpUrl,
		RpcUrl:  rpcUrl,
		ID:      id,
	})

	r.nextMemberId = common.Max(r.nextMemberId, id+1)

	if r.id != id {
		r.newMembers <- common.ClusterMemberChange{
			ClusterMember: common.ClusterMember{
				ID:      id,
				RpcUrl:  rpcUrl,
				HttpUrl: httpUrl,
			},
			Add: true,
		}

		if r.state == common.StateLeader {
			r.nextIndex[id] = r.persistState.LogLength() + 1 // data race
			r.matchIndex[id] = 0
		}
	}

	return nil
}

func (r *RaftBrainImpl) notifyNewMember(member common.ClusterMember) {
	r.nextMemberId = common.Max(r.nextMemberId, member.ID+1)

	if r.id != member.ID {
		r.newMembers <- common.ClusterMemberChange{
			ClusterMember: common.ClusterMember{
				ID:      member.ID,
				RpcUrl:  member.RpcUrl,
				HttpUrl: member.HttpUrl,
			},
			Add: true,
		}

		if r.state == common.StateLeader {
			r.nextIndex[member.ID] = r.persistState.LogLength() + 1 // data race
			r.matchIndex[member.ID] = 0
		}
	}
}
