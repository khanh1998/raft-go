package logic

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"time"

	"github.com/rs/zerolog/log"
)

func (r *RaftBrainImpl) isValidAddRequest(input common.AddServerInput) error {
	for _, mem := range r.Members {
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
	for _, mem := range r.Members {
		if mem.ID == input.ID && mem.HttpUrl == input.NewServerHttpUrl && mem.RpcUrl == input.NewServerRpcUrl {
			return errors.New("can't found the server to remove")
		}
	}
	return nil
}

func (r *RaftBrainImpl) RemoveServer(input common.RemoveServerInput, output *common.RemoveServerOutput) (err error) {
	r.InOutLock.Lock()
	_ = output // to disable the warning: argument output is overwritten before first use

	isLeader := r.State == common.StateLeader
	if !isLeader {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   "not leader",
		}

		r.InOutLock.Unlock()
		return nil
	}

	if err := r.isValidRemoveRequest(input); err != nil {
		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.InOutLock.Unlock()
		return nil
	}

	r.InOutLock.Unlock()

	if !r.ChangeMemberLock.TryLock() {
		err = errors.New("the server requested to be removed from the cluster is not exist")

		*output = common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.ChangeMemberLock.Unlock()

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
		Command:     common.ComposeRemoveServerCommand(input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

	// a leader that is removed from the configuration steps down once the C-new entry is committed.
	// If the leader stepped down before this point,
	// it might still time out and become leader again, delaying progress
	isLeaderRemoved := isLeader && input.ID == r.ID
	if isLeaderRemoved {
		for {
			if r.CommitIndex >= index {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		r.toFollower() // a follower without a membership can't issue request votes
	}

	*output = common.RemoveServerOutput{
		Status:     common.StatusOK,
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) AddServer(input common.AddServerInput, output *common.AddServerOutput) (err error) {
	r.InOutLock.Lock()
	if r.State != common.StateLeader {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   "not leader",
		}

		r.InOutLock.Unlock()
		return nil
	}

	if err := r.isValidAddRequest(input); err != nil {
		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		r.InOutLock.Unlock()
		return nil
	}

	r.InOutLock.Unlock()

	if !r.ChangeMemberLock.TryLock() {
		err = errors.New("another node is adding (removing) to the cluster")

		*output = common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
			Response:   err.Error(),
		}

		return nil
	}
	defer r.ChangeMemberLock.Unlock()
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
			if duration < time.Duration(r.ElectionTimeOutMin*1000) {
				break
			}
		}
		log.Info().Msgf("AddServer catchup end round %d", i)
	}

	// append new configuration to log (old configuration + new server), commit it using majority of new configuration
	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
		Command:     common.ComposeAddServerCommand(input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

	r.nextMemberId = input.ID + 1

	err = r.RpcProxy.SendToVotingMember(input.ID, &timeout)
	if err != nil {
		log.Err(err).Msg("SendToVotingMember")
	}

	var status common.ClientRequestStatus = common.StatusOK
	var response any = nil

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("ClientRequest_Register")
	}

	response, err = r.ARM.TakeResponse(index, 60*time.Second)
	if err != nil {
		status = common.StatusNotOK

		if errors.Is(err, common.ErrorSessionExpired) {
			response = common.SessionExpired
		}

		r.log().Err(err).Msg("ClientRequest_TakeResponse")
	}

	_ = response

	*output = common.AddServerOutput{
		Status: status,
	}
	// reply ok

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

			prevLog, err := r.getLog(nextIdx - 1)
			if err == nil {
				input.PrevLogTerm = prevLog.Term
			}
		}

		logItem, err := r.getLog(nextIdx)
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
	addition := false

	_, _, _, err := common.DecomposeAddSeverCommand(command)
	if err != nil {
		_, _, _, err = common.DecomposeRemoveServerCommand(command)
		if err != nil {
			return err
		}
	} else {
		addition = true
	}

	// revert change
	if addition {
		if err := r.removeMember(command); err != nil {
			return err
		}
	} else {
		if err := r.addMember(command); err != nil {
			return err
		}
	}

	return nil
}

func (r *RaftBrainImpl) changeMember(command string) error {
	if err := r.removeMember(command); err != nil {
		if err := r.addMember(command); err != nil {
			return err
		}
	}
	return nil
}

func (r *RaftBrainImpl) removeMember(command string) error {
	id, httpUrl, rpcUrl, err := common.DecomposeRemoveServerCommand(command)
	if err != nil {
		return err
	}

	tmp := []common.ClusterMember{}
	for _, mem := range r.Members {
		if mem.ID != id {
			tmp = append(tmp, mem)
		}
	}
	r.Members = tmp

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

func (r *RaftBrainImpl) addMember(command string) error {
	id, httpUrl, rpcUrl, err := common.DecomposeAddSeverCommand(command)
	if err != nil {
		return err
	}

	r.Members = append(r.Members, common.ClusterMember{
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

func (r *RaftBrainImpl) restoreClusterMemberInfoFromLogs() error {
	r.Members = []common.ClusterMember{}

	for _, log := range r.Logs {
		// log is always either add or remove
		_ = r.addMember(log.Command.(string))
		_ = r.removeMember(log.Command.(string))
	}

	return nil
}
