package logic

import "khanh/raft-go/common"

func (n *NodeImpl) DeleteLogFrom(index int) error {
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
	n.StateMachine.Reset()

	return nil
}

func (n *NodeImpl) AppendLogs(logItems []common.Log) {
	defer func() {
		data := n.Serialize(true, true, "AppendLogs")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLogs save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItems...)
}

func (n *NodeImpl) AppendLog(logItem common.Log) {
	defer func() {
		data := n.Serialize(true, true, "AppendLog")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLog save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItem)
}

func (n NodeImpl) GetLog(index int) (common.Log, error) {
	if len(n.Logs) == 0 {
		return common.Log{}, ErrLogIsEmtpy
	}

	if index > len(n.Logs) || index <= 0 {
		return common.Log{}, ErrIndexOutOfRange
	}

	realIndex := index - 1

	return n.Logs[realIndex], nil
}

func (n *NodeImpl) SetCurrentTerm(term int) {
	defer func() {
		data := n.Serialize(true, true, "SetCurrentTerm")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetCurrentTerm save to db error: ")
		}
	}()

	n.CurrentTerm = term
}

func (n *NodeImpl) SetVotedFor(nodeID int) {
	defer func() {
		data := n.Serialize(true, true, "SetVotedFor")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("SetVotedFor save to db error: ")
		}
	}()

	n.VotedFor = nodeID
}

func (n *NodeImpl) SetRpcProxy(rpc RPCProxy) {
	n.RpcProxy = rpc
}

func (n *NodeImpl) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
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
func (n *NodeImpl) applyLog() {
	n.log().Info().
		Interface("state_machine", n.StateMachine.GetData()).
		Interface("logs", n.Logs).
		Msg("applyLog: before")

	for n.CommitIndex > n.LastApplied {
		n.LastApplied += 1

		log, err := n.GetLog(n.LastApplied)
		if err != nil {
			break
		}

		for _, val := range log.Values {
			n.StateMachine.Put(val)
		}
	}

	n.log().Info().
		Interface("state_machine", n.StateMachine.GetData()).
		Interface("logs", n.Logs).
		Msg("applyLog: after")
}

func (n *NodeImpl) lastLogInfo() (index, term int) {
	if len(n.Logs) > 0 {
		index = len(n.Logs) - 1
		term = n.Logs[index].Term

		return index + 1, term
	}

	return 0, -1
}
