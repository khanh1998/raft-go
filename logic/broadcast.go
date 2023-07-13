package logic

import (
	"errors"
	"sync"
	"time"
)

func (n *NodeImpl) CallWithTimeout(clientIdx int, serviceMethod string, args any, reply any, timeout time.Duration) error {
	call := n.Peers[clientIdx].Go(serviceMethod, args, reply, nil)
	select {
	case <-time.After(timeout):
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			n.Reconnect(clientIdx)
			return resp.Error
		}
	}

	return nil
}

func (n *NodeImpl) BroadCastRequestVote() {
	n.resetElectionTimeout()
	if n.State == StateFollower {
		n.log().Info().Msg("BroadCastRequestVote")
		n.SetCurrentTerm(n.CurrentTerm + 1)
		n.ToCandidate()
		n.SetVotedFor(n.ID)

		lastLogIndex, lastLogTerm := n.lastLogInfo()
		input := RequestVoteInput{
			Term:         n.CurrentTerm,
			CandidateID:  n.ID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		responses := make([]*RequestVoteOutput, len(n.PeerURLs))
		voteGrantedCount := 1 // voted for itself first
		maxTerm := n.CurrentTerm
		maxTermID := -1

		var count sync.WaitGroup

		for peerIdx := range n.Peers {
			count.Add(1)
			go func(peerID int) {
				output := &RequestVoteOutput{}
				err := n.CallWithTimeout(peerID, "NodeImpl.RequestVote", input, output, 5*time.Second)
				if err != nil {
					n.log().Err(err).Msg("Client invocation error: ")
				} else {
					n.log().Info().Interface("response", output).Msg("Received response")
					responses[peerID] = output

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
			}(peerIdx)
		}

		count.Wait()

		n.log().Info().Interface("responses", responses).Msg("BroadCastRequestVote: Response")

		if voteGrantedCount > n.Quorum {
			n.ToLeader()
			n.log().Info().Msg("become leader")
		} else if maxTerm > n.CurrentTerm {
			n.ToFollower()
			n.SetVotedFor(maxTermID)
			n.log().Info().Msgf("follower of node %v", maxTermID)
		} else {
			n.ToFollower()
			n.SetVotedFor(0)
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

func (n *NodeImpl) BroadcastAppendEntries() {
	n.resetHeartBeatTimeout()
	if n.State == StateLeader {
		n.log().Info().Msg("BroadcastAppendEntries")

		successCount := 0
		maxTerm := n.CurrentTerm
		maxTermID := -1
		responses := make([]*AppendEntriesOutput, len(n.PeerURLs))

		var count sync.WaitGroup
		for peerID := range n.Peers {
			count.Add(1)
			go func(peerIndex int) {
				nextIdx := n.NextIndex[peerIndex]
				input := AppendEntriesInput{
					Term:         n.CurrentTerm,
					LeaderID:     n.ID,
					LeaderCommit: n.CommitedIndex,
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
					input.Entries = logItem.Values
				}

				output := &AppendEntriesOutput{}
				if err := n.CallWithTimeout(peerIndex, "NodeImpl.AppendEntries", input, output, 5*time.Second); err != nil {
					n.log().Err(err).Msg("BroadcastAppendEntries: ")
				} else {
					responses[peerIndex] = output

					if output.Success && output.Term > n.CurrentTerm {
						n.log().Fatal().Interface("response", output).Msg("inconsistent response")
					} else if output.Success {
						successCount += 1
						n.MatchIndex[peerIndex] = n.NextIndex[peerIndex]

						n.NextIndex[peerIndex] = min(n.NextIndex[peerIndex]+1, len(n.Logs)+1)
					} else {
						if output.Term <= n.CurrentTerm {
							n.NextIndex[peerIndex] = max(0, n.NextIndex[peerIndex]-1)
						} else {
							maxTerm = output.Term
							maxTermID = output.NodeID
						}
					}
				}

				count.Done()
			}(peerID)
		}
		count.Wait()

		n.log().Info().
			Int("ID", n.ID).
			Int("success_count", successCount).
			Int("max_term", maxTerm).
			Int("max_term_id", maxTermID).
			Interface("responses", responses).
			Msg("BroadcastAppendEntries")

		if successCount >= n.Quorum {
			n.CommitedIndex = len(n.Logs)
		} else if maxTerm > n.CurrentTerm {
			n.SetCurrentTerm(maxTerm)
			n.SetVotedFor(maxTermID)
			n.ToFollower()
		} else {
			n.SetCurrentTerm(maxTerm)
			n.SetVotedFor(0)
			n.ToFollower()
		}
	} else {
		// n.log().Info().Msg("BroadcastAppendEntries: not a leader")
	}
}
