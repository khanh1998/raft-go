package logic

import (
	"khanh/raft-go/common"
	"sync"
	"time"
)

func (n *RaftBrainImpl) BroadCastRequestVote() {
	n.logger.Info().Msg("BroadCastRequestVote")

	n.InOutLock.Lock()
	defer n.InOutLock.Unlock()

	if n.State == common.StateFollower {
		// On conversion to candidate, start election:
		// Increment currentTerm
		// Vote for self
		// Reset election timer
		// Send RequestVote RPCs to all other servers
		n.resetElectionTimeout()
		n.log().Info().Msg("BroadCastRequestVote")
		n.setCurrentTerm(n.CurrentTerm + 1)
		n.toCandidate()
		n.setVotedFor(n.ID)

		lastLogIndex, lastLogTerm := n.lastLogInfo()
		input := common.RequestVoteInput{
			Term:         n.CurrentTerm,
			CandidateID:  n.ID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		responses := make(map[int]*common.RequestVoteOutput, len(n.Peers))
		responsesMutex := sync.Mutex{}

		voteGrantedCount := 1 // voted for itself first
		maxTerm := n.CurrentTerm
		maxTermID := -1

		var count sync.WaitGroup

		timeout := 1 * time.Second

		for _, peer := range n.Peers {
			count.Add(1)
			go func(peerID int) {
				output, err := n.RpcProxy.SendRequestVote(peerID, &timeout, input)
				if err != nil {
					n.log().Err(err).Msg("Client invocation error: ")
				} else {
					n.log().Info().Interface("response", output).Msg("Received response")

					responsesMutex.Lock()
					defer responsesMutex.Unlock()

					responses[peerID] = &output

					if output.Term > n.CurrentTerm && output.VoteGranted {
						n.log().Fatal().Interface("output", output).Msg("inconsistent response")
					} else if output.Term > maxTerm {
						maxTerm = output.Term
						maxTermID = output.NodeID
					} else if output.VoteGranted {
						voteGrantedCount += 1
					}
				}

				count.Done()
			}(peer.ID)
		}

		count.Wait()

		n.log().Info().Interface("responses", responses).Msg("BroadCastRequestVote: Response")

		// TODO: If AppendEntries RPC received from new leader: convert to follower
		if voteGrantedCount > n.Quorum {
			n.toLeader()
			n.resetHeartBeatTimeout()
			n.log().Info().Msg("become leader")
		} else if maxTerm > n.CurrentTerm {
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
			n.toFollower()
			n.setVotedFor(maxTermID)
			n.log().Info().Msgf("follower of node %v", maxTermID)
		} else {
			n.toFollower()
			n.setVotedFor(0)
			n.log().Info().Msg("back to follower")
		}
	} else {
		// n.log().Info().
		// 	Interface("ID", n.ID).
		// 	Interface("state", n.State).
		// 	Interface("voted_for", n.VotedFor).
		// 	Msg("BroadCastRequestVote: not a follower")
	}
}

func (n *RaftBrainImpl) BroadcastAppendEntries() (majorityOK bool) {
	n.logger.Info().Msg("BroadcastAppendEntries")

	n.InOutLock.Lock()
	defer n.InOutLock.Unlock()

	if n.State == common.StateLeader {
		n.resetHeartBeatTimeout()
		n.log().Info().Msg("BroadcastAppendEntries")

		successCount := 0
		maxTerm := n.CurrentTerm
		maxTermID := -1
		m := map[int]common.AppendEntriesOutput{}
		responseLock := sync.Mutex{}

		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		//   follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		//   decrement nextIndex and retry (§5.3)

		var count sync.WaitGroup
		for _, peer := range n.Peers {
			count.Add(1)
			go func(peerID int) {
				nextIdx := n.NextIndex[peerID]
				input := common.AppendEntriesInput{
					Term:         n.CurrentTerm,
					LeaderID:     n.ID,
					LeaderCommit: n.CommitIndex,
				}

				if nextIdx > 1 {
					input.PrevLogIndex = nextIdx - 1

					prevLog, err := n.getLog(nextIdx - 1)
					if err == nil {
						input.PrevLogTerm = prevLog.Term
					}
				}

				logItem, err := n.getLog(nextIdx)
				if err == nil {
					input.Entries = []common.Log{logItem}
				}

				timeout := 1 * time.Second

				output, err := n.RpcProxy.SendAppendEntries(peerID, &timeout, input)
				if err != nil {
					n.log().Err(err).Msg("BroadcastAppendEntries: ")
				} else {
					responseLock.Lock()
					defer responseLock.Unlock()

					m[peerID] = output

					if output.Success && output.Term > n.CurrentTerm {
						n.log().Fatal().Interface("response", output).Msg("inconsistent response")
					} else if output.Success {
						successCount += 1
						n.MatchIndex[peerID] = common.Min(n.NextIndex[peerID], len(n.Logs))

						n.NextIndex[peerID] = common.Min(n.NextIndex[peerID]+1, len(n.Logs)+1)
					} else {
						if output.Term <= n.CurrentTerm {
							n.NextIndex[peerID] = common.Max(0, n.NextIndex[peerID]-1)
						} else {
							// the appendEntries request is failed,
							// because current leader is outdated
							maxTerm = output.Term
							maxTermID = output.NodeID
						}
					}
				}

				count.Done()
			}(peer.ID)
		}

		count.Wait()

		n.log().Info().
			Int("ID", n.ID).
			Int("success_count", successCount).
			Int("max_term", maxTerm).
			Int("max_term_id", maxTermID).
			Interface("responses", m).
			Msg("BroadcastAppendEntries")

		if successCount >= n.Quorum {
			majorityOK = true
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			for N := len(n.Logs); N > n.CommitIndex; N++ {

				log, err := n.getLog(N)
				if err == nil {
					count := 1 // count for itself
					for _, matchIndex := range n.MatchIndex {
						if matchIndex >= N {
							count += 1
						}
					}

					if count >= n.Quorum && log.Term == n.CurrentTerm {
						n.CommitIndex = N

						break
					}
				}
			}
		} else if maxTerm > n.CurrentTerm {
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
			n.setCurrentTerm(maxTerm)
			n.setVotedFor(maxTermID)
			n.toFollower()
		} else {
			// n.SetCurrentTerm(maxTerm)
			// n.SetVotedFor(0)
			// n.ToFollower()
		}

		n.applyLog()
	} else {
		// n.log().Info().Msg("BroadcastAppendEntries: not a leader")
	}

	return
}
