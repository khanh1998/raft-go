package logic

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"os"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

func (r *RaftBrainImpl) isValidAddRequest(input common.AddServerInput) error {
	for _, mem := range r.members {
		if mem.ID == input.ID || mem.HttpUrl == input.NewServerHttpUrl || mem.RpcUrl == input.NewServerRpcUrl {
			return errors.New("the server's information is duplicated")
		}
	}
	if input.ID < r.nextMemberId {
		return fmt.Errorf("server is should be %d or bigger than that", r.nextMemberId)
	}
	return nil
}

func (r *RaftBrainImpl) isValidRemoveRequest(input common.RemoveServerInput) error {
	for _, mem := range r.members {
		if mem.ID == input.ID && mem.HttpUrl == input.NewServerHttpUrl && mem.RpcUrl == input.NewServerRpcUrl {
			return nil
		}
	}
	return errors.New("can't found the server to remove")
}

func (r *RaftBrainImpl) WaitForLogCommited(dur time.Duration, index int) error {
	timeout := time.After(dur)
	stop := false
	for !stop {
		select {
		case <-timeout:
			return errors.New("timeout: wait for log commited")
		default:
			if r.CommitIndex >= index {
				stop = true
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func (r *RaftBrainImpl) RemoveServer(input common.RemoveServerInput, output *common.RemoveServerOutput) (err error) {
	r.inOutLock.Lock()
	_ = output // to disable the warning: argument output is overwritten before first use

	isLeader := r.State == common.StateLeader
	if !isLeader {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   "not leader",
		}

		r.inOutLock.Unlock()
		return nil
	}

	if err := r.isValidRemoveRequest(input); err != nil {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.inOutLock.Unlock()
		return nil
	}

	r.inOutLock.Unlock()

	if !r.changeMemberLock.TryLock() {
		err = errors.New("the server requested to be removed from the cluster is not exist")

		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.changeMemberLock.Unlock()

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     common.ComposeRemoveServerCommand(input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

	msg := fmt.Sprintf("server %d can be shut down now", input.ID)
	err = r.WaitForLogCommited(30*time.Second, index)
	if err != nil {
		msg = fmt.Sprintf("log isn't commited, server %d can't be shut down", input.ID)
	}

	isLeaderRemoved := isLeader && input.ID == r.ID
	if isLeaderRemoved {
		r.State = common.StateRemoved
		r.Stop <- struct{}{}
		r.log().Info().Msg("the server will be shut down in 5s")
		time.AfterFunc(5*time.Second, func() {
			pid := os.Getpid()
			syscall.Kill(pid, syscall.SIGTERM)
		})
	}

	*output = common.RemoveServerOutput{
		Status:     common.StatusOK,
		LeaderHint: "",
		Response:   msg,
	}

	return nil
}

func (r *RaftBrainImpl) AddServer(input common.AddServerInput, output *common.AddServerOutput) (err error) {
	r.inOutLock.Lock()
	if r.State != common.StateLeader {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   "not leader",
		}

		r.inOutLock.Unlock()
		return nil
	}

	if err := r.isValidAddRequest(input); err != nil {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.inOutLock.Unlock()
		return nil
	}

	r.inOutLock.Unlock()

	if !r.changeMemberLock.TryLock() {
		err = errors.New("another node is adding (removing) to the cluster")

		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.changeMemberLock.Unlock()
	timeout := 5 * time.Second

	if err := r.RpcProxy.ConnectToNewPeer(input.ID, input.NewServerRpcUrl, 5, timeout); err != nil {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		log.Err(err).Msg("ConnectToNewPeer")

		return nil
	}

	// catch up new server for fixed number of rounds
	for i := 0; i < 10; i++ {
		log.Info().Msgf("AddServer catchup start round %d", i)
		begin := time.Now()

		err := r.catchUpWithNewMember(input.ID)
		if err != nil {
			*output = common.AddServerOutput{
				Status:     common.StatusNotOK,
				LeaderHint: r.getLeaderHttpUrl(),
				Response:   err.Error(),
			}

			return nil
		} else {
			duration := time.Since(begin)
			if duration < time.Duration(r.electionTimeOutMin*1000) {
				break
			}
		}
		log.Info().Msgf("AddServer catchup end round %d", i)
	}

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     common.ComposeAddServerCommand(input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

	// after append new config to log, we use it immediately without wating commit

	r.nextMemberId = input.ID + 1

	// allow new server to become follower
	err = r.RpcProxy.SendToVotingMember(input.ID, &timeout)
	if err != nil {
		log.Err(err).Msg("SendToVotingMember")
	}

	// wait for log to committed
	msg := ""
	err = r.WaitForLogCommited(30*time.Second, index)
	if err != nil {
		msg = fmt.Sprintf("log isn't commited, server %d isn't surely joined the cluster", input.ID)
	}

	*output = common.AddServerOutput{
		Status:   common.StatusOK,
		Response: msg,
	}

	return nil
}

func (r *RaftBrainImpl) catchUpWithNewMember(peerID int) error {
	initNextIdx := len(r.Logs)
	nextIdx := initNextIdx
	currentTerm := r.CurrentTerm
	matchIndex := 0

	for matchIndex < initNextIdx {

		input := common.AppendEntriesInput{
			Term:         currentTerm,
			LeaderID:     r.ID,
			LeaderCommit: r.CommitIndex,
		}

		if nextIdx > 1 {
			input.PrevLogIndex = nextIdx - 1

			prevLog, err := r.GetLog(nextIdx - 1)
			if err == nil {
				input.PrevLogTerm = prevLog.Term
			}
		}

		logItem, err := r.GetLog(nextIdx)
		if err == nil {
			input.Entries = []common.Log{logItem}
		}

		timeout := 5 * time.Second

		output, err := r.RpcProxy.SendAppendEntries(peerID, &timeout, input)
		r.log().Debug().Interface("output", output).Msg("r.RpcProxy.SendAppendEntries")
		if err != nil {
			r.log().Err(err).Msg("BroadcastAppendEntries: ")
		} else {
			if output.Success && output.Term > currentTerm {
				r.log().Fatal().Interface("response", output).Msg("inconsistent response")
			} else if output.Success {
				matchIndex = common.Min(nextIdx, initNextIdx)

				nextIdx = common.Min(nextIdx+1, initNextIdx+1)
			} else {
				if output.Term <= r.CurrentTerm {
					nextIdx = common.Max(0, nextIdx-1)
				} else {
					// the appendEntries request is failed,
					// because current leader is outdated
					err = errors.New("the follower cannot be more up to date than the current leader")
					r.log().Error().Msg(err.Error())
					return err
				}
			}
		}
	}

	r.log().Info().
		Interface("matchIndex", matchIndex).
		Interface("initNextIdx", initNextIdx).
		Msg("finish catch up")

	return nil
}

func (r *RaftBrainImpl) revertChangeMember(command string) error {
	addition, id, httpUrl, rpcUrl, err := common.DecomposeChangeSeverCommand(command)
	if err != nil {
		return err
	}

	// revert change
	if addition {
		return r.removeMember(id, httpUrl, rpcUrl)
	} else {
		return r.addMember(id, httpUrl, rpcUrl)
	}
}

func (r *RaftBrainImpl) changeMember(command string) error {
	addition, id, httpUrl, rpcUrl, err := common.DecomposeChangeSeverCommand(command)
	if err != nil {
		return err
	}

	if addition {
		return r.addMember(id, httpUrl, rpcUrl)
	} else {
		return r.removeMember(id, httpUrl, rpcUrl)
	}
}

func (r *RaftBrainImpl) removeMember(id int, httpUrl, rpcUrl string) error {
	tmp := []common.ClusterMember{}
	for _, mem := range r.members {
		if mem.ID != id {
			tmp = append(tmp, mem)
		}
	}
	r.members = tmp

	if r.ID != id {
		r.newMembers <- common.ClusterMemberChange{
			ClusterMember: common.ClusterMember{
				ID:      id,
				RpcUrl:  rpcUrl,
				HttpUrl: httpUrl,
			},
			Add: false,
		}

		if r.State == common.StateLeader {
			delete(r.NextIndex, id)
			delete(r.MatchIndex, id)
		}
	}

	return nil
}

func (r *RaftBrainImpl) addMember(id int, httpUrl, rpcUrl string) error {
	r.members = append(r.members, common.ClusterMember{
		HttpUrl: httpUrl,
		RpcUrl:  rpcUrl,
		ID:      id,
	})

	r.nextMemberId = common.Max(r.nextMemberId, id+1)

	if r.ID != id {
		r.newMembers <- common.ClusterMemberChange{
			ClusterMember: common.ClusterMember{
				ID:      id,
				RpcUrl:  rpcUrl,
				HttpUrl: httpUrl,
			},
			Add: true,
		}

		if r.State == common.StateLeader {
			r.NextIndex[id] = len(r.Logs) + 1 // data race
			r.MatchIndex[id] = 0
		}
	}

	return nil
}

func (r *RaftBrainImpl) restoreClusterMemberInfoFromLogs() (err error) {
	r.members = []common.ClusterMember{}

	for _, log := range r.Logs {
		err = r.changeMember(log.Command.(string))
		if err != nil {
			r.log().Err(err).Msg("restoreClusterMemberInfoFromLogs")
		}
	}

	return nil
}
