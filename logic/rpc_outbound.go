package logic

import (
	"context"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
)

func (n *RaftBrainImpl) BroadCastRequestVote(ctx context.Context) {
	startTime := time.Now()
	ctx, span := tracer.Start(ctx, "BroadCastRequestVote")
	defer span.End()

	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	n.resetElectionTimeout(ctx)

	if n.state != common.StateFollower {
		span.SetStatus(codes.Ok, "not a follower, don't request vote")
		return
	}
	// On conversion to candidate, start election:
	// Increment currentTerm
	// Vote for self
	// Reset election timer
	// Send RequestVote RPCs to all other servers
	n.setCurrentTerm(ctx, n.currentTerm+1)
	n.toCandidate(ctx)
	n.setVotedFor(ctx, n.id)

	span.AddEvent("to candidate")

	lastLogIndex, lastLogTerm := n.lastLogInfo()
	input := common.RequestVoteInput{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	responses := make(map[int]*common.RequestVoteOutput, len(n.members))
	responsesMutex := sync.Mutex{}

	voteGrantedCount := 1 // voted for itself first
	maxTerm := n.currentTerm
	maxTermID := n.id

	var count sync.WaitGroup

	timeout := n.RpcRequestTimeout

	for _, peer := range n.members {
		if peer.ID == n.id {
			continue
		}

		count.Add(1)
		go func(peerID int) {
			defer count.Done()

			output, err := n.rpcProxy.SendRequestVote(ctx, peerID, &timeout, input)
			if err != nil {
				n.log().ErrorContext(ctx, "n.rpcProxy.SendRequestVote", err, "peerId", peerID, "input", input)
			} else {
				responsesMutex.Lock()
				defer responsesMutex.Unlock()

				responses[peerID] = &output

				if output.Term > maxTerm {
					maxTerm = output.Term
					maxTermID = output.NodeID
				} else if output.VoteGranted {
					voteGrantedCount += 1
				}
			}
		}(peer.ID)
	}

	count.Wait()

	if voteGrantedCount >= n.Quorum() {
		n.toLeader(ctx)
		n.resetHeartBeatTimeout(ctx)
		n.resetElectionTimeout(ctx)

		span.AddEvent("to leader")
	} else {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		n.toFollower(ctx)
		n.setVotedFor(ctx, 0)
		n.setLeaderID(ctx, 0)
		n.setCurrentTerm(ctx, maxTerm)

		span.AddEvent("to follower")
	}

	n.log().InfoContext(ctx,
		"BroadCastRequestVote",
		"responses", responses,
		"members", n.members,
		"voteGrantedCount", voteGrantedCount,
		"maxTerm", maxTerm,
		"maxTermId", maxTermID,
		"quorum", n.Quorum(),
	)

	span.SetStatus(codes.Ok, "finish send request vote")
	duration := time.Since(startTime)
	observability.SetRequestVoteDuration(ctx, duration)
}

func (n *RaftBrainImpl) BroadcastAppendEntries(ctx context.Context) (majorityOK bool) {
	start := time.Now()
	ctx, span := tracer.Start(ctx, "BroadcastAppendEntries")
	defer span.End()

	// TODO: shorten the critical region, only acquire lock when reading or writing data.
	// in the paper, when a node are acting as a candidate, if it receive request with higher term,
	// it will step down as follower. it can't be achieved with this implemenation.
	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	if n.state != common.StateLeader {
		span.SetStatus(codes.Ok, "not a leader, don't send append entries")
		return false
	}

	n.resetHeartBeatTimeout(ctx)
	successCount := 1 // count how many servers it can communicate to
	maxTerm := n.currentTerm
	maxTermID := n.id
	m := map[int]common.AppendEntriesOutput{}
	responseLock := sync.Mutex{}

	// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
	// • If successful: update nextIndex and matchIndex for
	//   follower (§5.3)
	// • If AppendEntries fails because of log inconsistency:
	//   decrement nextIndex and retry (§5.3)

	var count sync.WaitGroup
	for _, peer := range n.members {
		if peer.ID == n.id {
			continue
		}

		count.Add(1)
		go func(peerID int) {
			defer count.Done()

			nextIdx := n.nextIndex[peerID]
			input := common.AppendEntriesInput{
				Term:         n.currentTerm,
				LeaderID:     n.id,
				LeaderCommit: n.commitIndex,
			}

			if nextIdx > 1 {
				input.PrevLogIndex = nextIdx - 1

				prevLog, err := n.GetLog(nextIdx - 1)
				if err == nil {
					input.PrevLogTerm = prevLog.Term
				}
			}

			logItem, err := n.GetLog(nextIdx)
			if err == nil {
				input.Entries = []common.Log{logItem}
			}

			timeout := n.RpcRequestTimeout

			output, err := n.rpcProxy.SendAppendEntries(ctx, peerID, &timeout, input)
			if err != nil {
				n.log().ErrorContext(ctx, "BroadcastAppendEntries", err)
			} else {
				responseLock.Lock()
				defer responseLock.Unlock()
				m[peerID] = output
				// event if follower responds success=false, we still consider it as success
				// because the leader are making progress
				successCount += 1

				if output.Success {
					n.matchIndex[peerID] = common.Min(n.nextIndex[peerID], len(n.logs))
					n.nextIndex[peerID] = common.Min(n.nextIndex[peerID]+1, len(n.logs)+1) // data race
				} else {
					if output.Term <= n.currentTerm {
						n.nextIndex[peerID] = common.Max(0, n.nextIndex[peerID]-1) // data race
					} else {
						// the appendEntries request is failed,
						// because current leader is outdated
						maxTerm = output.Term
						maxTermID = output.NodeID
					}
				}
			}
		}(peer.ID)
	}

	count.Wait()

	if maxTerm > n.currentTerm {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		n.setCurrentTerm(ctx, maxTerm)
		n.setVotedFor(ctx, 0)
		n.setLeaderID(ctx, 0)
		n.toFollower(ctx)

		span.AddEvent("to follower")
	} else if successCount >= n.Quorum() {
		majorityOK = true
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		for N := len(n.logs); N > n.commitIndex; N-- {
			log, err := n.GetLog(N)
			if err != nil {
				break
			}

			count := 1 // count for itself
			for _, matchIndex := range n.matchIndex {
				if matchIndex >= N {
					count += 1
				}
			}

			if count >= n.Quorum() && log.Term == n.currentTerm {
				n.commitIndex = N

				break
			}
		}

		span.AddEvent("majority ok")
	}

	n.log().InfoContext(ctx,
		"BroadcastAppendEntries",
		"successCount", successCount,
		"maxTerm", maxTerm,
		"maxTermID", maxTermID,
		"responses", m,
		"members", n.members,
		"majorityOk", majorityOK,
		"quorum", n.Quorum(),
	)

	n.applyLog(ctx)

	span.SetStatus(codes.Ok, "finished send append entries")

	n.log().InfoContext(ctx, "BroadcastAppendEntries Done")

	duration := time.Since(start)
	observability.SetAppendEntriesDuration(ctx, duration)

	return
}
