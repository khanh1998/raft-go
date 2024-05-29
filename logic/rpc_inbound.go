package logic

import (
	"khanh/raft-go/common"
	"time"
)

var (
	MsgRequesterTermIsOutDated              = "the node sent the request is out dated"
	MsgPreviousLogTermsAreNotMatched        = "previous log terms are not matched"
	MsgCurrentLogTermsAreNotMatched         = "current log terms are not matched"
	MsgTheResponderHasNoLog                 = "the responder has no log (empty)"
	MsgTheResponderHasFewerLogThanRequester = "the responder has fewer log than the requester"
	MsgTheResponderAlreadyMakeAVote         = "the responder already made a vote"
	MsgTheRequesterLogsAreOutOfDate         = "the requestor logs are out of date"
	MsgTheLeaderIsStillAlive                = "the leader is still alive"
)

// AppendEntries Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (n *RaftBrainImpl) AppendEntries(input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	defer func() {
		n.log().Info().
			Interface("id", n.ID).
			Interface("input", input).
			Interface("output", output).
			Msg("AppendEntries")
	}()

	n.InOutLock.Lock()
	defer n.InOutLock.Unlock()

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.CurrentTerm {
		*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgRequesterTermIsOutDated, NodeID: n.ID}

		return nil
	}

	n.lastHeartbeatReceivedTime = time.Now()
	n.resetElectionTimeout()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > n.CurrentTerm {
		n.toFollower()
		n.setLeaderID(input.LeaderID)
		n.setCurrentTerm(input.Term)
		n.setVotedFor(0)
	}

	if input.Term == n.CurrentTerm {
		n.setLeaderID(input.LeaderID)
	}

	// WARN: log index start from 1, not 0
	if input.PrevLogIndex > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		logItem, err := n.getLog(input.PrevLogIndex)
		switch err {
		case ErrLogIsEmtpy:
			*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgTheResponderHasNoLog, NodeID: n.ID}

			return nil
		case ErrIndexOutOfRange:
			*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgTheResponderHasFewerLogThanRequester, NodeID: n.ID}

			return nil
		case nil:
			if logItem.Term != input.PrevLogTerm {
				*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgPreviousLogTermsAreNotMatched, NodeID: n.ID}

				return nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		logItem, err = n.getLog(input.PrevLogIndex + 1)
		if err == nil {
			if logItem.Term != input.Term {
				n.deleteLogFrom(input.PrevLogIndex + 1)
				*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: false, Message: MsgCurrentLogTermsAreNotMatched, NodeID: n.ID}

				return nil
			}
		}
	}

	// 4. Append any new entries not already in the log
	if len(input.Entries) > 0 {
		_, err = n.getLog(input.PrevLogIndex + 1)
		if err != nil { // entries are not already in the log
			n.appendLogs(input.Entries)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > n.CommitIndex {
		n.CommitIndex = common.Min(input.LeaderCommit, len(n.Logs)) // data race
	}

	n.applyLog()

	*output = common.AppendEntriesOutput{Term: n.CurrentTerm, Success: true, Message: "", NodeID: n.ID}

	return nil
}

// Invoked by candidates to gather votes (§5.2).
func (n *RaftBrainImpl) RequestVote(input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	defer func() {
		n.log().Info().
			Interface("id", n.ID).
			Interface("input", input).
			Interface("output", output).
			Msg("RequestVote")
	}()

	n.InOutLock.Lock()
	defer n.InOutLock.Unlock()

	// when a follower is removed from the cluster, it will never aware of that,
	// because the leader won't send update logs to removed follower after it receive the received command.
	// this check is to prevent removed follower request vote from others.
	if time.Since(n.lastHeartbeatReceivedTime) < time.Duration(n.ElectionTimeOutMax*1000000) {
		output = &common.RequestVoteOutput{
			Term:        n.CurrentTerm,
			Message:     MsgTheLeaderIsStillAlive,
			VoteGranted: false,
			NodeID:      n.ID,
		}
		return
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.CurrentTerm {
		*output = common.RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgRequesterTermIsOutDated, NodeID: n.ID}

		return nil
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > n.CurrentTerm {
		n.setCurrentTerm(input.Term)
		n.setLeaderID(0)
		n.setVotedFor(0)
		n.toFollower()
		// if current node is a follower: then no need to convert to follower.
		// if current node is a candidate:
		// if current node is a leader:
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if n.VotedFor == 0 || n.VotedFor == input.CandidateID {
		// Retry Mechanisms: However, in a distributed system, network partitions, retries due to lost messages, or other anomalies might lead to a scenario where the same candidate might end up re-sending a RequestVote RPC, or where a follower might process a duplicate RequestVote.
		if n.isLogUpToDate(input.LastLogIndex, input.LastLogTerm) {
			n.setVotedFor(input.CandidateID)
			n.resetElectionTimeout()

			*output = common.RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: true, Message: "", NodeID: n.ID}

			return nil
		} else {
			*output = common.RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate, NodeID: n.ID}

			return nil
		}
	} else {
		*output = common.RequestVoteOutput{Term: n.CurrentTerm, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote, NodeID: n.ID}

		return nil
	}
}
