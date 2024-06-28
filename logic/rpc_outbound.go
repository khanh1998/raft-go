package logic

import (
	"khanh/raft-go/common"
	"sync"
	"time"
)

func (n *RaftBrainImpl) BroadCastRequestVote() {
	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	n.resetElectionTimeout()

	if n.state != common.StateFollower {
		return
	}
	// On conversion to candidate, start election:
	// Increment currentTerm
	// Vote for self
	// Reset election timer
	// Send RequestVote RPCs to all other servers
	n.setCurrentTerm(n.currentTerm + 1)
	n.toCandidate()
	n.setVotedFor(n.id)

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

	timeout := 150 * time.Millisecond

	for _, peer := range n.members {
		if peer.ID == n.id {
			continue
		}

		count.Add(1)
		go func(peerID int) {
			defer count.Done()

			output, err := n.rpcProxy.SendRequestVote(peerID, &timeout, input)
			if err != nil {
				n.log().Err(err).Msg("Client invocation error: ")
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
		n.toLeader()
		n.resetHeartBeatTimeout()
		n.resetElectionTimeout()
	} else {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		n.toFollower()
		n.setVotedFor(0)
		n.setLeaderID(0)
		n.setCurrentTerm(maxTerm)
	}

	n.log().Info().
		Interface("responses", responses).
		Interface("members", n.members).
		Int("vote_granted_count", voteGrantedCount).
		Int("max_term", maxTerm).
		Int("max_term_id", maxTermID).
		Int("quorum", n.Quorum()).
		Msg("BroadCastRequestVote")

}

func (n *RaftBrainImpl) BroadcastAppendEntries() (majorityOK bool) {
	// TODO: shorten the critical region, only acquire lock when reading or writing data.
	// in the paper, when a node are acting as a candidate, if it receive request with higher term,
	// it will step down as follower. it can't be achieved with this implemenation.
	n.inOutLock.Lock()
	defer n.inOutLock.Unlock()

	if n.state != common.StateLeader {
		return false
	}

	n.resetHeartBeatTimeout()
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

			timeout := 150 * time.Millisecond

			output, err := n.rpcProxy.SendAppendEntries(peerID, &timeout, input)
			if err != nil {
				n.log().Err(err).Msg("BroadcastAppendEntries: ")
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
		n.setCurrentTerm(maxTerm)
		n.setVotedFor(0)
		n.setLeaderID(0)
		n.toFollower()
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
	}

	n.log().Info().
		Int("success_count", successCount).
		Int("max_term", maxTerm).
		Int("max_term_id", maxTermID).
		Interface("responses", m).
		Interface("members", n.members).
		Bool("majority_ok", majorityOK).
		Int("quorum", n.Quorum()).
		Msg("BroadcastAppendEntries")

	n.applyLog()

	n.log().Info().Msg("BroadcastAppendEntries Done")

	return
}
