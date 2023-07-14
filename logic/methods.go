package logic

import "fmt"

var (
	MsgRequesterTermIsOutDated              = "the node sent the request is out dated"
	MsgPreviousLogTermsAreNotMatched        = "previous log terms are not matched"
	MsgCurrentLogTermsAreNotMatched         = "current log terms are not matched"
	MsgTheResponderHasNoLog                 = "the responder has no log (empty)"
	MsgTheResponderHasFewerLogThanRequester = "the responder has fewer log than the requester"
	MsgTheResponderAlreadyMakeAVote         = "the responder already made a vote"
	MsgTheRequesterLogsAreOutOfDate         = "the requestor logs are out of date"
)

func (n *NodeImpl) Ping(name string, message *string) (err error) {
	*message = fmt.Sprintf("Hello %s from %v", name, n.ID)
	return nil
}

// AppendEntries Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (n *NodeImpl) AppendEntries(input *AppendEntriesInput, output *AppendEntriesOutput) (err error) {
	n.log().Info().
		Interface("ID", n.ID).
		Interface("req", input).
		Msg("Received an AppendEntries request")

	defer func() {
		output.NodeID = n.ID
		if output.Success {
			n.resetElectionTimeout()
		}

		n.log().Info().
			Interface("ID", n.ID).
			Interface("out", output).
			Msg("Responsed an AppendEntries request")
	}()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > n.CurrentTerm {
		n.CurrentTerm = input.Term
		n.ToFollower()
		n.SetVotedFor(input.LeaderID)
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.CurrentTerm {
		*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgRequesterTermIsOutDated}

		return nil
	}

	if input.PrevLogIndex > 0 { // if leader has no log, then no need to check
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		// WARN: log index start from 1, not 0
		logItem, err := n.GetLog(input.PrevLogIndex)
		switch err {
		case ErrLogIsEmtpy:
			*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgTheResponderHasNoLog}

			return nil
		case ErrIndexOutOfRange:
			*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgTheResponderHasFewerLogThanRequester}

			return nil
		case nil:
			if logItem.Term != input.PrevLogTerm {
				*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgPreviousLogTermsAreNotMatched}

				return nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		logItem, err = n.GetLog(input.PrevLogIndex + 1)
		if err == nil {
			if logItem.Term != input.Term {
				n.DeleteLogFrom(input.PrevLogIndex + 1)
				*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgCurrentLogTermsAreNotMatched}

				return nil
			}
		}
	}

	// 4. Append any new entries not already in the log
	if len(input.Entries) > 0 {
		logItem := Log{
			Term:   n.CurrentTerm,
			Values: make([]Entry, len(input.Entries)),
		}

		copy(logItem.Values, input.Entries)
		n.AppendLog(logItem)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > n.CommitIndex {
		n.CommitIndex = min(input.LeaderCommit, len(n.Logs))
	}

	n.applyLog()

	*output = AppendEntriesOutput{Term: n.CurrentTerm, Success: true, Message: ""}

	return nil
}

// Invoked by candidates to gather votes (§5.2).
func (n *NodeImpl) RequestVote(input *RequestVoteInput, output *RequestVoteOutput) (err error) {
	n.log().Info().
		Interface("ID", n.ID).
		Interface("req", input).
		Msg("Received an RequestVote request")

	defer func() {
		output.NodeID = n.ID

		if output.VoteGranted {
			n.resetElectionTimeout()
		}

		n.log().Info().
			Interface("ID", n.ID).
			Interface("out", output).
			Msg("Response an RequestVote request")
	}()

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.CurrentTerm {
		*output = RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgRequesterTermIsOutDated}

		return nil
	}
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if n.VotedFor == 0 {
		if n.isLogUpToDate(input.LastLogIndex, input.LastLogTerm) {
			n.SetVotedFor(input.CandidateID)

			*output = RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: true, Message: ""}

			return nil
		} else {
			*output = RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate}

			return nil
		}
	} else {
		*output = RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote}

		return nil
	}
}
