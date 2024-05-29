package logic

import (
	"khanh/raft-go/common"
	"math"
	"time"
)

func (n *RaftBrainImpl) isMemberOfCluster() bool {
	for _, mem := range n.Members {
		if mem.ID == n.ID {
			return true
		}
	}
	return false
}

func (n *RaftBrainImpl) deleteLogFrom(index int) error {
	defer func() {
		data := n.serialize(true, true, "DeleteLogFrom")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("DeleteLogFrom save to db error: ")
		}
	}()

	if len(n.Logs) == 0 {
		return ErrLogIsEmtpy
	}

	if index > len(n.Logs) || index <= 0 {
		return ErrIndexOutOfRange
	}

	realIndex := int(index - 1)
	deletedLogs := n.Logs[realIndex:]
	n.Logs = n.Logs[:realIndex]

	// these two numbers will be calculated again later.
	n.LastApplied = 0
	n.CommitIndex = 0
	// clear all data in state machine, so logs can be applied from beginning later.
	n.StateMachine = common.NewKeyValueStateMachine()

	for i := len(deletedLogs) - 1; i >= 0; i-- {
		n.revertChangeMember(deletedLogs[i].Command.(string))
	}

	return nil
}

func (n *RaftBrainImpl) appendLogs(logItems []common.Log) {
	defer func() {
		data := n.serialize(true, true, "AppendLogs")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLogs save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItems...)

	// we need to update cluster membership infomation as soon as we receive the log,
	// don't need to wait until it get commited.
	for _, logItem := range logItems {
		n.changeMember(logItem.Command.(string))
	}
}

func (n *RaftBrainImpl) appendLog(logItem common.Log) int {
	defer func() {
		data := n.serialize(true, true, "AppendLog")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLog save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItem)
	index := len(n.Logs)

	n.log().Info().Interface("log", logItem).Msg("AppendLog")

	// we need to update cluster membership infomation as soon as we receive the log,
	// don't need to wait until it get commited.
	n.changeMember(logItem.Command.(string))

	return index
}

func (n *RaftBrainImpl) getLog(index int) (common.Log, error) {
	if len(n.Logs) == 0 {
		return common.Log{}, ErrLogIsEmtpy
	}

	if index > len(n.Logs) || index <= 0 {
		return common.Log{}, ErrIndexOutOfRange
	}

	realIndex := index - 1

	return n.Logs[realIndex], nil
}

func (n *RaftBrainImpl) setLeaderID(leaderId int) {
	defer func() {
		data := n.serialize(true, true, "SetLeaderID")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetLeaderID save to db error: ")
		}
	}()

	n.LeaderID = leaderId
}

func (n *RaftBrainImpl) setCurrentTerm(term int) {
	defer func() {
		data := n.serialize(true, true, "SetCurrentTerm")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetCurrentTerm save to db error: ")
		}
	}()

	n.CurrentTerm = term
}

func (n *RaftBrainImpl) setVotedFor(nodeID int) {
	defer func() {
		data := n.serialize(true, true, "SetVotedFor")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetVotedFor save to db error: ")
		}
	}()

	n.VotedFor = nodeID
}

func (n *RaftBrainImpl) SetRpcProxy(rpc RPCProxy) {
	n.RpcProxy = rpc
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
func (n *RaftBrainImpl) applyLog() {
	for n.CommitIndex > n.LastApplied {
		n.LastApplied += 1

		log, err := n.getLog(n.LastApplied)
		if err != nil {
			break
		}

		res, _ := n.StateMachine.Process(log.ClientID, log.SequenceNum, log.Command, n.LastApplied)

		if n.State == common.StateLeader {
			err = n.ARM.PutResponse(n.LastApplied, res, err, 30*time.Second)
			if err != nil {
				n.log().Err(err).Msg("ApplyLog_PutResponse")
			} else {
				n.log().Info().
					Interface("log", log).
					Msg("applyLog_PutResponse")
			}
		}
	}
}

func (r *RaftBrainImpl) Quorum() int {
	return int(math.Floor(float64(len(r.Members))/2.0)) + 1
}

func (n *RaftBrainImpl) lastLogInfo() (index, term int) {
	if len(n.Logs) > 0 {
		index = len(n.Logs) - 1
		term = n.Logs[index].Term

		return index + 1, term
	}

	return 0, -1
}

func (r *RaftBrainImpl) GetMembers() []common.ClusterMember {
	return r.Members
}

func (r *RaftBrainImpl) GetNewMembersChannel() <-chan common.ClusterMemberChange {
	return r.newMembers
}
