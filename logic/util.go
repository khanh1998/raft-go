package logic

import (
	"context"
	"khanh/raft-go/common"
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

func (n *RaftBrainImpl) deleteLogTo(index int) (err error) {
	if len(n.logs) == 0 {
		return ErrLogIsEmtpy
	}

	if index > len(n.logs) || index <= 0 {
		return ErrIndexOutOfRange
	}

	realIndex := int(index - 1)
	n.logs = n.logs[realIndex+1:]

	return nil
}

func (n *RaftBrainImpl) deleteLogFrom(ctx context.Context, index int) (err error) {
	defer func() {
		data := n.serialize(true, true, "DeleteLogFrom")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "DeleteLogFrom save to db error: ", err)
		}
	}()

	if len(n.logs) == 0 {
		return ErrLogIsEmtpy
	}

	if index > len(n.logs) || index <= 0 {
		return ErrIndexOutOfRange
	}

	realIndex := int(index - 1)
	deletedLogs := n.logs[realIndex:]
	n.logs = n.logs[:realIndex]

	// these two numbers will be calculated again later.
	n.lastApplied = 0
	n.commitIndex = 0
	// clear all data in state machine, reload latest snapshot from file,
	// so logs can be applied from beginning again.
	err = n.stateMachine.Reset() // TODO: in log compaction, no need to to this.
	if err != nil {
		return err
	}

	for i := len(deletedLogs) - 1; i >= 0; i-- {
		n.revertChangeMember(deletedLogs[i].Command.(string))
	}

	return nil
}

func (n *RaftBrainImpl) appendLogs(ctx context.Context, logItems []common.Log) {
	defer func() {
		data := n.serialize(true, true, "AppendLogs")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "DeleteLogFrom save to db error: ", err)
		}
	}()

	n.logs = append(n.logs, logItems...)

	// we need to update cluster membership infomation as soon as we receive the log,
	// don't need to wait until it get commited.
	for _, logItem := range logItems {
		n.changeMember(logItem.Command.(string))
	}
}

func (n *RaftBrainImpl) appendLog(ctx context.Context, logItem common.Log) int {
	defer func() {
		data := n.serialize(true, true, "AppendLog")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "AppendLog save to db error: ", err)
		}
	}()

	n.logs = append(n.logs, logItem)
	index := len(n.logs)

	n.log().InfoContext(ctx, "AppendLog", "log", logItem)

	// we need to update cluster membership infomation as soon as we receive the log,
	// don't need to wait until it get commited.
	n.changeMember(logItem.Command.(string))

	return index
}

func (n *RaftBrainImpl) GetLog(index int) (common.Log, error) {
	if len(n.logs) == 0 {
		return common.Log{}, ErrLogIsEmtpy
	}

	if index > len(n.logs) || index <= 0 {
		return common.Log{}, ErrIndexOutOfRange
	}

	realIndex := index - 1

	return n.logs[realIndex], nil
}

func (n *RaftBrainImpl) setLeaderID(ctx context.Context, leaderId int) {
	defer func() {
		data := n.serialize(true, true, "SetLeaderID")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "SetLeaderID save to db error: ", err)
		}
	}()

	n.leaderID = leaderId
}

func (n *RaftBrainImpl) setCurrentTerm(ctx context.Context, term int) {
	defer func() {
		data := n.serialize(true, true, "SetCurrentTerm")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "SetCurrentTerm save to db error: ", err)
		}
	}()

	n.currentTerm = term
}

func (n *RaftBrainImpl) setVotedFor(ctx context.Context, nodeID int) {
	defer func() {
		data := n.serialize(true, true, "SetVotedFor")
		if err := n.db.AppendLog(data); err != nil {
			n.log().ErrorContext(ctx, "SetVotedFor save to db error: ", err)
		}
	}()

	n.votedFor = nodeID
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

// All servers: If commitIndex > lastApplied: increment lastApplied,
// apply log[lastApplied] to state machine (§5.3)
func (n *RaftBrainImpl) applyLog(ctx context.Context) {
	for n.commitIndex > n.lastApplied {
		n.lastApplied += 1

		log, err := n.GetLog(n.lastApplied)
		if err != nil {
			n.log().ErrorContext(ctx, "applyLog_GetLog", err)
			break
		}

		n.clusterClock.NewEpoch(log.ClusterTime)

		res, err := n.stateMachine.Process(log.ClientID, log.SequenceNum, log.Command, n.lastApplied, log.ClusterTime)

		if n.state == common.StateLeader {
			err = n.arm.PutResponse(n.lastApplied, res, err, 30*time.Second)
			if err != nil {
				n.log().ErrorContext(ctx, "applyLog_PutResponse", err)
			} else {
				n.log().InfoContext(ctx, "applyLog_PutResponse", "log", log)
			}
		}

		if err != nil {
			n.log().ErrorContext(ctx, "applyLog_Process", err)
		}
	}
}

func (r *RaftBrainImpl) Quorum() int {
	return int(math.Floor(float64(len(r.members))/2.0)) + 1
}

func (n *RaftBrainImpl) lastLogInfo() (index, term int) {
	if len(n.logs) > 0 {
		index = len(n.logs) - 1
		term = n.logs[index].Term

		return index + 1, term
	}

	return 0, -1
}

func (r *RaftBrainImpl) GetNewMembersChannel() <-chan common.ClusterMemberChange {
	return r.newMembers
}
