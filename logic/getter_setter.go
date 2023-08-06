package logic

import "khanh/raft-go/common"

func (n *RaftBrainImpl) DeleteLogFrom(index int) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	defer func() {
		data := n.Serialize(true, true, "DeleteLogFrom")
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
	n.Logs = n.Logs[:realIndex]

	// these two numbers will be calculated again later.
	n.LastApplied = 0
	n.CommitIndex = 0
	// clear all data in state machine, so logs can be applied from beggining later.
	n.StateMachine = common.NewKeyValueStateMachine()

	return nil
}

func (n *RaftBrainImpl) AppendLogs(logItems []common.Log) {
	defer func() {
		data := n.Serialize(true, true, "AppendLogs")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLogs save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItems...)
}

func (n *RaftBrainImpl) AppendLog(logItem common.Log) int {
	defer func() {
		data := n.Serialize(true, true, "AppendLog")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLog save to db error: ")
		}
	}()

	n.lock.Lock()
	n.Logs = append(n.Logs, logItem)
	index := len(n.Logs)
	n.lock.Unlock()

	return index
}

func (n RaftBrainImpl) GetLog(index int) (common.Log, error) {
	if len(n.Logs) == 0 {
		return common.Log{}, ErrLogIsEmtpy
	}

	if index > len(n.Logs) || index <= 0 {
		return common.Log{}, ErrIndexOutOfRange
	}

	realIndex := index - 1

	return n.Logs[realIndex], nil
}

func (n *RaftBrainImpl) SetCurrentTerm(term int) {
	defer func() {
		data := n.Serialize(true, true, "SetCurrentTerm")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetCurrentTerm save to db error: ")
		}
	}()

	n.CurrentTerm = term
}

func (n *RaftBrainImpl) SetVotedFor(nodeID int) {
	defer func() {
		data := n.Serialize(true, true, "SetVotedFor")
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

		log, err := n.GetLog(n.LastApplied)
		if err != nil {
			break
		}

		res, err := n.StateMachine.Process(log.ClientID, log.SequenceNum, log.Command, n.LastApplied)

		if n.State == StateLeader {
			err = n.ARM.PutResponse(n.LastApplied, res, err)
			if err != nil {
				n.log().Err(err).Msg("ApplyLog")
			}
		}
	}
}

func (n *RaftBrainImpl) lastLogInfo() (index, term int) {
	if len(n.Logs) > 0 {
		index = len(n.Logs) - 1
		term = n.Logs[index].Term

		return index + 1, term
	}

	return 0, -1
}
