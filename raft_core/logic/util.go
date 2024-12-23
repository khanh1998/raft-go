package logic

import (
	"context"
	gc "khanh/raft-go/common"
	"khanh/raft-go/raft_core/common"
	"math"
	"time"
)

func (n *RaftBrainImpl) isMemberOfCluster(id *int) bool {
	if id == nil {
		id = &n.id
	}

	for _, mem := range n.members {
		if mem.ID == *id {
			return true
		}
	}
	return false
}

func (n *RaftBrainImpl) deleteLogFrom(ctx context.Context, index int) (err error) {
	deletedLogs, err := n.persistState.DeleteLogFrom(ctx, index)
	if err != nil {
		return err
	}

	n.resetClusterTime()

	// clear all data in state machine, reload latest snapshot from file,
	// so logs can be applied from beginning again.
	// err = n.stateMachine.Reset(ctx) // TODO: in log compaction, no need to to this.
	// if err != nil {
	// 	return err
	// }

	// no need to reset the state machine, because only committed logs are push into state machine,
	// and committed logs can't be deleted

	// figure out which index to begin re-applying the logs
	// snapshot := n.persistState.GetLatestSnapshotMetadata()
	// these two numbers will be calculated again later.
	// n.lastApplied = snapshot.LastLogIndex
	// n.commitIndex = snapshot.LastLogIndex

	for i := len(deletedLogs) - 1; i >= 0; i-- {
		n.revertChangeMember(deletedLogs[i])
	}

	return nil
}

func (n *RaftBrainImpl) appendLogs(ctx context.Context, logItems []gc.Log) (err error) {
	for _, logItem := range logItems {
		err := n.changeMember(logItem)
		if err != nil {
			n.log().ErrorContext(ctx, "appendLog_changeMember", err, "command", logItem)
		}
	}

	_, err = n.persistState.AppendLog(ctx, logItems)
	if err != nil {
		return err
	}

	n.log().InfoContext(ctx, "AppendLogs", "logs", logItems)

	return nil
}

func (n *RaftBrainImpl) appendLog(ctx context.Context, logItem gc.Log) (int, error) {
	// we need to update cluster membership information as soon as we receive the log,
	// don't need to wait until it get committed.
	n.changeMember(logItem)

	lastLog, err := n.persistState.GetLastLog()
	if err == nil {
		if lastLog.GetTime() > logItem.GetTime() {
			n.log().FatalContext(ctx, "next", logItem, "prev", lastLog)
		}
	}

	index, err := n.persistState.AppendLog(ctx, []gc.Log{logItem})
	if err != nil {
		return 0, err
	}

	n.log().InfoContext(ctx, "AppendLog", "log", logItem, "index", index)

	return index, nil
}

func (n *RaftBrainImpl) lastLogInfo() (index, term int) {
	return n.persistState.LastLogInfo()
}

// if the index equals to lastIndex of the latest snapshot,
// return lastIndex, and lastTerm as the log, and an special error to indicate that the log is in snapshot
func (n *RaftBrainImpl) GetLog(index int) (gc.Log, error) {
	return n.persistState.GetLog(index)
}

func (n *RaftBrainImpl) GetLastLog() (gc.Log, error) {
	return n.persistState.GetLastLog()
}

func (n *RaftBrainImpl) setLeaderID(ctx context.Context, leaderId int) {
	n.leaderID = leaderId
}

func (n *RaftBrainImpl) setCurrentTerm(ctx context.Context, term int) error {
	return n.persistState.SetCurrentTerm(ctx, term)
}

func (n *RaftBrainImpl) setVotedFor(ctx context.Context, nodeID int) error {
	return n.persistState.SetVotedFor(ctx, nodeID)
}

func (n *RaftBrainImpl) SetRpcProxy(rpc RPCProxy) {
	n.rpcProxy = rpc
}

func (n *RaftBrainImpl) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	index, term := n.lastLogInfo()
	if lastLogTerm > term {
		return true
	} else if lastLogTerm == term && lastLogIndex >= index {
		return true
	} else {
		return false
	}
}

// this function will pick committed logs and push it to state machine
func (n *RaftBrainImpl) logInjector(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.heartBeatTimeOutMax):
			n.applyLog(ctx)
		}
	}
}

// All servers: If commitIndex > lastApplied: increment lastApplied,
// apply log[lastApplied] to state machine (§5.3)
func (n *RaftBrainImpl) applyLog(ctx context.Context) {
	commitIndex := n.commitIndex
	for commitIndex > n.lastApplied {
		n.lastApplied += 1

		log, err := n.GetLog(n.lastApplied)
		if err != nil {
			n.log().ErrorContext(ctx, "applyLog_GetLog", err)
			break
		}

		if n.state != gc.StateLeader {
			n.clusterClock.NewEpoch(log.GetTime())
		}

		res, err := n.stateMachine.Process(ctx, n.lastApplied, log)

		if n.state == gc.StateLeader {
			err1 := n.arm.PutResponse(n.lastApplied, res, err, 30*time.Second)
			if err1 != nil {
				n.log().ErrorContext(ctx, "applyLog_PutResponse", err)
			} else {
				n.log().InfoContext(ctx, "applyLog_PutResponse", "log", log, "err", err)
			}
		}

		// rethink about the logic here to better prevent consecutive snapshot requests,
		// can't use lastApplied > limit
		if n.lastApplied%n.logLengthLimit == 0 {
			if err = n.stateMachine.StartSnapshot(ctx); err != nil {
				n.log().ErrorContext(ctx, "applyLog_startSnapshot", err)
			}
		}
	}
}

func (r *RaftBrainImpl) Quorum() int {
	return int(math.Floor(float64(len(r.members))/2.0)) + 1
}

func (r *RaftBrainImpl) GetNewMembersChannel() <-chan common.ClusterMemberChange {
	return r.newMembers
}
