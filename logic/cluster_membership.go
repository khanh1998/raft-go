package logic

import (
	"errors"
	"khanh/raft-go/common"
	"time"

	"github.com/rs/zerolog/log"
)

func (r *RaftBrainImpl) RemoveServer(input common.RemoveServerInput, output *common.RemoveServerOutput) (err error) {
	r.log().Info().Msg("RemoveServer get called")
	// do input verify
	r.InOutLock.Lock()
	isLeader := r.State == common.StateLeader

	if !isLeader {
		output = &common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		r.InOutLock.Unlock()
		return nil
	}
	r.InOutLock.Unlock()

	if !r.ChangeMemberLock.TryLock() {
		output = &common.RemoveServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		return errors.New("another node is adding (removing) to the cluster")
	}
	defer r.ChangeMemberLock.Unlock()
	log.Info().Msg("AddServer Got the key")

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
		Command:     common.ComposeRemoveServerCommand(input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

	if isLeader {
		for {
			if r.CommitIndex >= index {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		r.toFollower()
	}

	output = &common.RemoveServerOutput{
		Status:     common.StatusOK,
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) AddServer(input common.AddServerInput, output *common.AddServerOutput) (err error) {
	// TODO: check whether the server is added to the cluster or not
	r.InOutLock.Lock()
	if r.State != common.StateLeader {
		output = &common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		r.InOutLock.Unlock()
		return nil
	}
	r.InOutLock.Unlock()

	if !r.ChangeMemberLock.TryLock() {
		output = &common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		return errors.New("another node is adding (removing) to the cluster")
	}
	defer r.ChangeMemberLock.Unlock()
	log.Info().Msg("AddServer Got the key")
	timeout := 5 * time.Second

	if err := r.RpcProxy.ConnectToNewPeer(input.ID, input.NewServerRpcUrl, 5, timeout); err != nil {
		output = &common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		return err
	}

	log.Info().Msg("AddServer ConnectToNewPeer")

	// catch up new server for fixed number of rounds
	for i := 0; i < 10; i++ {
		log.Info().Msgf("AddServer catchup start round %d", i)
		begin := time.Now()

		err := r.CatchUpWithNewMember(input.ID)
		if err != nil {
			return err
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

func (r *RaftBrainImpl) CatchUpWithNewMember(peerID int) error {
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

func (r *RaftBrainImpl) RevertChangeMember(command string) error {
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
		if err := r.RemoveMember(command); err != nil {
			return err
		}
	} else {
		if err := r.AddMember(command); err != nil {
			return err
		}
	}

	return nil
}
func (r *RaftBrainImpl) ChangeMember(command string) error {
	if err := r.RemoveMember(command); err != nil {
		if err := r.AddMember(command); err != nil {
			return err
		}
	}
	return nil
}

func (r *RaftBrainImpl) RemoveMember(command string) error {
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

func (r *RaftBrainImpl) AddMember(command string) error {
	id, httpUrl, rpcUrl, err := common.DecomposeAddSeverCommand(command)
	if err != nil {
		return err
	}

	r.Members = append(r.Members, common.ClusterMember{
		HttpUrl: httpUrl,
		RpcUrl:  rpcUrl,
		ID:      id,
	})

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

func (r *RaftBrainImpl) RestoreClusterMemberInfoFromLogs() error {
	r.Members = []common.ClusterMember{}
	addition := true

	for _, log := range r.Logs {
		id, httpUrl, rpcUrl, err := common.DecomposeAddSeverCommand(log.Command.(string))
		if err != nil {
			id, httpUrl, rpcUrl, err = common.DecomposeAddSeverCommand(log.Command.(string))
			if err != nil {
				continue
			}
		} else {
			addition = true
		}

		r.Members = append(r.Members, common.ClusterMember{
			HttpUrl: httpUrl,
			RpcUrl:  rpcUrl,
			ID:      id,
		})

		if r.ID != id {
			r.newMembers <- common.ClusterMemberChange{
				ClusterMember: common.ClusterMember{
					ID:      id,
					RpcUrl:  rpcUrl,
					HttpUrl: httpUrl,
				},
				Add: addition,
			}
		}
	}

	return nil
}
