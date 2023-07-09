package logic

import (
	"errors"
	"net/rpc"
	"sync"
	"time"
)

func (n *NodeImpl) CallWithTimeout(client *rpc.Client, serviceMethod string, args any, reply any, timeout time.Duration) error {
	call := client.Go(serviceMethod, args, reply, nil)
	select {
	case <-time.After(timeout):
		return errors.New("RPC timeout")
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
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

		var responses []*RequestVoteOutput

		var count sync.WaitGroup

		for _, client := range n.Peers {
			count.Add(1)
			go func(client *rpc.Client) {
				output := &RequestVoteOutput{}
				err := n.CallWithTimeout(client, "NodeImpl.RequestVote", input, output, 5*time.Second)
				if err != nil {
					n.log().Err(err).Msg("Client invocation error: ")
				} else {
					n.log().Info().Interface("response", output).Msg("Received response")
					responses = append(responses, output)
				}

				count.Done()
			}(client)
		}

		count.Wait()

		voteGrantedCount := 1 // voted for itself first
		maxTerm := 0
		for _, item := range responses {
			if item.VoteGranted {
				voteGrantedCount += 1
			}

			maxTerm = max(item.Term, maxTerm)

		}

		n.log().Info().Interface("responses", responses).Msg("BroadCastRequestVote")

		if voteGrantedCount > n.Quorum {
			n.ToLeader()

			n.log().Info().Msg("become leader")
		} else if maxTerm > n.CurrentTerm {
			n.CurrentTerm = maxTerm
			n.ToFollower()
			n.SetVotedFor(0)
			n.log().Info().Msg("back to follower")
		} else {
			n.ToFollower()
			n.SetVotedFor(0)
			n.log().Info().Msg("back to follower")
		}
	} else {
		n.log().Info().
			Interface("ID", n.ID).
			Interface("state", n.State).
			Interface("voted_for", n.VotedFor).
			Msg("BroadCastRequestVote: can't request")
	}
}

func (n *NodeImpl) BroadcastAppendEntries() {
	n.resetHeartBeatTimeout()
	if n.State == StateLeader {
		n.log().Info().Msg("BroadcastAppendEntries")
		successCount := 0
		var count sync.WaitGroup
		for peerID, client := range n.Peers {
			count.Add(1)
			go func(peerID int, client *rpc.Client) {
				nextIdx := n.NextIndex[peerID]
				input := AppendEntriesInput{
					Term:         n.CurrentTerm,
					LeaderID:     n.ID,
					LeaderCommit: n.CommitedIndex,
				}

				if nextIdx > 1 {
					input.PrevLogIndex = nextIdx - 1
					input.PrevLogTerm = n.Logs[nextIdx-1].Term
				}

				logItem, err := n.GetLog(nextIdx)
				if err != nil {
					input.Entries = logItem.Values
				}

				output := &AppendEntriesOutput{}
				if err := n.CallWithTimeout(client, "NodeImpl.AppendEntries", input, output, 5*time.Second); err != nil {
					n.log().Err(err).Msg("BroadcastAppendEntries: ")
				}

				if output.Success {
					successCount += 1
					n.MatchIndex[peerID] = n.NextIndex[peerID]

					n.NextIndex[peerID] = min(n.NextIndex[peerID]+1, len(n.Logs)+1)
				} else {
					if output.Term <= n.CurrentTerm {
						n.NextIndex[peerID] = max(0, n.NextIndex[peerID]-1)
					} else {
						n.ToFollower()
						n.SetVotedFor(0)
					}
				}

				n.log().Info().
					Interface("ID", n.ID).
					Interface("success_count", successCount).
					Interface("responses", output).
					Msg("BroadcastAppendEntries")

				count.Done()
			}(peerID, client)

		}
		count.Wait()
		if successCount >= n.Quorum {
			n.CommitedIndex = len(n.Logs)
		}
	} else {
		n.log().Info().Msg("BroadcastAppendEntries: can't request")
	}
}
