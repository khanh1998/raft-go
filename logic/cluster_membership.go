package logic

import (
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"time"
)

func (r *RaftBrainImpl) AddServer(input common.AddServerInput, output *common.AddServerOutput) (err error) {
	r.AddServerLock.Lock()
	defer r.AddServerLock.Unlock()

	if r.State != StateLeader {
		output = &common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}
	}

	if err := r.RpcProxy.ConnectToNewPeer(input.ID, input.NewServerRpcUrl, 5, 5*time.Second); err != nil {
		output = &common.AddServerOutput{
			Status:     common.StatusNotOK,
			LeaderHint: r.getLeaderHttpUrl(),
		}

		return err
	}

	// catch up new server for fixed number of rounds
	for i := 0; i < 10; i++ {
		begin := time.Now()

		err := r.CatchUp(input.ID)
		if err != nil {
			r.log().Err(err).Msg("AddServer_CatchUp")
		}

		duration := time.Since(begin)
		if err == nil && duration < time.Duration(r.MinRandomDuration*1000) {
			break
		}
	}

	// wait until the previous configuration in log is committed

	// append new configuration to log (old configuration + new server), commit it using majority of new configuration
	index := r.AppendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     fmt.Sprintf("addServer %d %s %s", input.ID, input.NewServerHttpUrl, input.NewServerRpcUrl),
	})

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
func (r *RaftBrainImpl) CatchUp(peerID int) error {
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
