package logic

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"time"

	"go.opentelemetry.io/otel/codes"
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
	MsgCannotAppendLog                      = "cannot append log"
	MsgCannotDeleteLog                      = "cannot delete log"
	MsgTheResponderSnapshotIsOutdated       = "the responder's snapshot is outdated"
)

// AppendEntries Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (n *RaftBrainImpl) AppendEntries(ctx context.Context, input *common.AppendEntriesInput, output *common.AppendEntriesOutput) (err error) {
	ctx, span := tracer.Start(ctx, "AppendEntries")
	defer span.End()

	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	currentTerm := n.GetCurrentTerm()

	defer func() {
		n.log().InfoContext(ctx, "AppendEntries", "input", input, "output", output)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "finished process append entries")
		}
	}()

	// if current leader get removed,
	// if follower get removed, it won't get these inbound methods revoked.
	if n.state == common.StateRemoved {
		return nil
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < currentTerm {
		*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgRequesterTermIsOutDated, NodeID: n.id}

		return nil
	}

	n.lastHeartbeatReceivedTime = time.Now()
	n.resetElectionTimeout(ctx)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > currentTerm {
		n.toFollower(ctx)
		n.setLeaderID(ctx, input.LeaderID)
		n.setCurrentTerm(ctx, input.Term)
		currentTerm = input.Term
		n.setVotedFor(ctx, 0)
	}

	if input.Term == currentTerm {
		n.setLeaderID(ctx, input.LeaderID)
	}

	// WARN: log index start from 1, not 0
	if input.PrevLogIndex > 0 {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		logItem, err := n.GetLog(input.PrevLogIndex)
		switch err {
		case common.ErrLogIsEmpty:
			*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgTheResponderHasNoLog, NodeID: n.id}

			return nil
		case common.ErrIndexOutOfRange:
			*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgTheResponderHasFewerLogThanRequester, NodeID: n.id}

			return nil
		case nil, common.ErrLogIsInSnapshot:
			if logItem.GetTerm() != input.PrevLogTerm {
				*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgPreviousLogTermsAreNotMatched, NodeID: n.id}

				return nil
			}
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		logItem, err = n.GetLog(input.PrevLogIndex + 1)
		if err == nil {
			if logItem.GetTerm() != input.Term {
				err := n.deleteLogFrom(ctx, input.PrevLogIndex+1)
				if err != nil {
					*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgCannotDeleteLog, NodeID: n.id}

					return nil
				}
				*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgCurrentLogTermsAreNotMatched, NodeID: n.id}

				return nil
			}
		}

		if err != nil && errors.Is(err, common.ErrLogIsInSnapshot) {
			if logItem.GetTerm() != input.Term {
				// delete latest snapshot
				*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgTheResponderSnapshotIsOutdated, NodeID: n.id}

			}
		}
	}

	// 4. Append any new entries not already in the log
	if len(input.Entries) > 0 {
		_, err = n.GetLog(input.PrevLogIndex + 1)
		if err != nil { // entries are not already in the log
			err := n.appendLogs(ctx, input.Entries)
			if err != nil {
				*output = common.AppendEntriesOutput{Term: currentTerm, Success: false, Message: MsgCannotAppendLog, NodeID: n.id}

				return nil
			}
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > n.commitIndex {
		n.commitIndex = common.Min(input.LeaderCommit, n.GetLogLength()) // data race
	}

	// n.applyLog(ctx)

	*output = common.AppendEntriesOutput{Term: currentTerm, Success: true, Message: "", NodeID: n.id}

	return nil
}

// Invoked by candidates to gather votes (§5.2).
func (n *RaftBrainImpl) RequestVote(ctx context.Context, input *common.RequestVoteInput, output *common.RequestVoteOutput) (err error) {
	ctx, span := tracer.Start(ctx, "RequestVote")
	defer span.End()

	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	currentTerm := n.GetCurrentTerm()
	votedFor := n.GetVotedFor()

	defer func() {
		n.log().InfoContext(ctx, "RequestVote", "input", input, "output", output)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "finished process request vote")
		}
	}()

	if n.state == common.StateRemoved {
		return nil
	}

	// when a follower is removed from the cluster, it will never aware of that,
	// because the leader won't send update logs to removed follower after it receive the received command.
	// this check is to prevent removed follower request vote from others.
	if n.state == common.StateFollower {
		if time.Since(n.lastHeartbeatReceivedTime) < n.electionTimeOutMin {
			output = &common.RequestVoteOutput{
				Term:        currentTerm,
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
				Term:        currentTerm,
				Message:     MsgTheLeaderIsStillAlive,
				VoteGranted: false,
				NodeID:      n.id,
			}
			return
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < currentTerm {
		*output = common.RequestVoteOutput{Term: currentTerm, VoteGranted: false, Message: MsgRequesterTermIsOutDated, NodeID: n.id}

		return nil
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > currentTerm {
		n.setCurrentTerm(ctx, input.Term)
		currentTerm = input.Term

		n.setLeaderID(ctx, 0)
		n.setVotedFor(ctx, 0)
		n.toFollower(ctx)
		// if current node is a follower: then no need to convert to follower.
		// if current node is a candidate:
		// if current node is a leader:
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if votedFor == 0 || votedFor == input.CandidateID {
		// Retry Mechanisms: However, in a distributed system, network partitions, retries due to lost messages, or other anomalies might lead to a scenario where the same candidate might end up re-sending a RequestVote RPC, or where a follower might process a duplicate RequestVote.
		if n.isLogUpToDate(input.LastLogIndex, input.LastLogTerm) {
			n.setVotedFor(ctx, input.CandidateID)
			n.resetElectionTimeout(ctx)

			*output = common.RequestVoteOutput{Term: currentTerm, VoteGranted: true, Message: "", NodeID: n.id}

			return nil
		} else {
			*output = common.RequestVoteOutput{Term: currentTerm, VoteGranted: false, Message: MsgTheRequesterLogsAreOutOfDate, NodeID: n.id}

			return nil
		}
	} else {
		*output = common.RequestVoteOutput{Term: currentTerm, VoteGranted: false, Message: MsgTheResponderAlreadyMakeAVote, NodeID: n.id}

		return nil
	}
}
