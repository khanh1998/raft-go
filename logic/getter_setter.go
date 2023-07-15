package logic

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

	return nil
}

func (n *NodeImpl) AppendLogs(logItems []Log) {
	defer func() {
		data := n.Serialize(true, true, "AppendLogs")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLogs save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItems...)
}

func (n *NodeImpl) AppendLog(logItem Log) {
	defer func() {
		data := n.Serialize(true, true, "AppendLog")
		if err := n.DB.AppendLog(data); err != nil {
			n.log().Err(err).Msg("AppendLog save to db error: ")
		}
	}()

	n.Logs = append(n.Logs, logItem)
}

func (n NodeImpl) GetLog(index int) (Log, error) {
	if len(n.Logs) == 0 {
		return Log{}, ErrLogIsEmtpy
	}

	if index > len(n.Logs) || index <= 0 {
		return Log{}, ErrIndexOutOfRange
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
