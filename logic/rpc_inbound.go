package logic

import (
	"context"
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
func (n *RaftBrainImpl) AppendEntries(ctx context.Context, input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	ctx, span := tracer.Start(ctx, "AppendEntries")
	defer span.End()

	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	defer func() {
		n.log(ctx).Info().
			Interface("id", n.id).
			Interface("input", input).
			Interface("output", output).
			Msg("AppendEntries")
	}()

	// if current leader get removed,
	// if follower get removed, it won't get these inbound methods revoked.
	if n.state == common.StateRemoved {
		return nil
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.currentTerm {
		*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: false, Message: MsgRequesterTermIsOutDated, NodeID: n.id}

		return nil
	}

	n.lastHeartbeatReceivedTime = time.Now()
	n.resetElectionTimeout(ctx)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > n.currentTerm {
		n.toFollower(ctx)
		n.setLeaderID(ctx, input.LeaderID)
		n.setCurrentTerm(ctx, input.Term)
		n.setVotedFor(ctx, 0)
	}

	if input.Term == n.currentTerm {
		n.setLeaderID(ctx, input.LeaderID)
	}

	// WARN: log index start from 1, not 0
	if input.PrevLogIndex > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		logItem, err := n.GetLog(input.PrevLogIndex)
		switch err {
		case ErrLogIsEmtpy:
			*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: false, Message: MsgTheResponderHasNoLog, NodeID: n.id}

			return nil
		case ErrIndexOutOfRange:
			*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: false, Message: MsgTheResponderHasFewerLogThanRequester, NodeID: n.id}

			return nil
		case nil:
			if logItem.Term != input.PrevLogTerm {
				*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: false, Message: MsgPreviousLogTermsAreNotMatched, NodeID: n.id}

				return nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		logItem, err = n.GetLog(input.PrevLogIndex + 1)
		if err == nil {
			if logItem.Term != input.Term {
				n.deleteLogFrom(ctx, input.PrevLogIndex+1)
				*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: false, Message: MsgCurrentLogTermsAreNotMatched, NodeID: n.id}

				return nil
			}
		}
	}

	// 4. Append any new entries not already in the log
	if len(input.Entries) > 0 {
		_, err = n.GetLog(input.PrevLogIndex + 1)
		if err != nil { // entries are not already in the log
			n.appendLogs(ctx, input.Entries)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > n.commitIndex {
		n.commitIndex = common.Min(input.LeaderCommit, len(n.logs)) // data race
	}

	n.applyLog(ctx)

	*output = common.AppendEntriesOutput{Term: n.currentTerm, Success: true, Message: "", NodeID: n.id}

	return nil
}

// Invoked by candidates to gather votes (§5.2).
func (n *RaftBrainImpl) RequestVote(ctx context.Context, input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	ctx, span := tracer.Start(ctx, "RequestVote")
	defer span.End()

	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	defer func() {
		n.log(ctx).Info().
			Interface("id", n.id).
			Interface("input", input).
			Interface("output", output).
			Msg("RequestVote")
	}()

	if n.state == common.StateRemoved {
		return nil
	}

	// when a follower is removed from the cluster, it will never aware of that,
	// because the leader won't send update logs to removed follower after it receive the received command.
	// this check is to prevent removed follower request vote from others.
	if n.state == common.StateFollower {
		if time.Since(n.lastHeartbeatReceivedTime) < time.Duration(n.electionTimeOutMin*1000000) {
			output = &common.RequestVoteOutput{
				Term:        n.currentTerm,
				Message:     MsgTheLeaderIsStillAlive,
				VoteGranted: false,
				NodeID:      n.id,
			}
			return
		}
	}

	if n.state == common.StateLeader {
		// leader always have latest membership information,
		// if the disrupt node is not a member of cluster, just reject it.
		if !n.isMemberOfCluster(&input.CandidateID) {
			output = &common.RequestVoteOutput{
				Term:        n.currentTerm,
				Message:     MsgTheLeaderIsStillAlive,
				VoteGranted: false,
				NodeID:      n.id,
			}
			return
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < n.currentTerm {
		*output = common.RequestVoteOutput{Term: n.currentTerm, VoteGranted: false, Message: MsgRequesterTermIsOutDated, NodeID: n.id}

		return nil
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > n.currentTerm {
		n.setCurrentTerm(ctx, input.Term)
		n.setLeaderID(ctx, 0)
		n.setVotedFor(ctx, 0)
		n.toFollower(ctx)
		// if current node is a follower: then no need to convert to follower.
		// if current node is a candidate:
		// if current node is a leader:
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if n.votedFor == 0 || n.votedFor == input.CandidateID {
		// Retry Mechanisms: However, in a distributed system, network partitions, retries due to lost messages, or other anomalies might lead to a scenario where the same candidate might end up re-sending a RequestVote RPC, or where a follower might process a duplicate RequestVote.
		if n.isLogUpToDate(input.LastLogIndex, input.LastLogTerm) {
			n.setVotedFor(ctx, input.CandidateID)
			n.resetElectionTimeout(ctx)

			*output = common.RequestVoteOutput{Term: n.currentTerm, VoteGranted: true, Message: "", NodeID: n.id}

			return nil
		} else {
			*output = common.RequestVoteOutput{Term: n.currentTerm, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate, NodeID: n.id}

			return nil
		}
	} else {
		*output = common.RequestVoteOutput{Term: n.currentTerm, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote, NodeID: n.id}

		return nil
	}
}
