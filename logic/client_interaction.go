package logic

import (
	"errors"
	"khanh/raft-go/common"
	"time"
)

func (r *RaftBrainImpl) getLeaderUrl() string {
	leaderUrl := ""
	for _, peer := range r.Peers {
		if peer.ID != r.ID && peer.ID == r.VotedFor {
			leaderUrl = peer.URL

			break
		}
	}

	return leaderUrl
}

func (r *RaftBrainImpl) ClientRequest(input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error) {
	if r.State != StateLeader {
		leaderUrl := r.getLeaderUrl()

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		return nil
	}

	index := r.AppendLog(common.Log{
		Term:    r.CurrentTerm,
		Command: input.Command,
	})

	r.BroadcastAppendEntries()

	var status common.ClientRequestStatus = common.StatusOK
	var response any = nil

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("ClientRequest_Register")
	}

	response, err = r.ARM.TakeResponse(index, 60*time.Second)
	if err != nil {
		status = common.StatusNotOK

		if err == common.ErrorSessionExpired {
			response = common.SessionExpired
		}

		r.log().Err(err).Msg("ClientRequest_TakeResponse")
	}

	*output = common.ClientRequestOutput{
		Status:   status,
		Response: response,
	}

	return nil
}

func (r *RaftBrainImpl) RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error) {
	if r.State != StateLeader {
		leaderUrl := r.getLeaderUrl()

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: leaderUrl,
		}

		return nil
	}

	index := r.AppendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     "register",
	})

	r.BroadcastAppendEntries()

	var status common.ClientRequestStatus = common.StatusOK

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("RegisterClient_Register")
	}

	_, err = r.ARM.TakeResponse(index, 60*time.Second)
	if err != nil {
		status = common.StatusNotOK
	}

	*output = common.RegisterClientOutput{
		Status:     status,
		ClientID:   index,
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {
	defer func() {
		if err != nil {
			output.Status = common.StatusNotOK
		}
	}()
	if r.State != StateLeader {
		leaderUrl := r.getLeaderUrl()

		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			LeaderHint: leaderUrl,
		}

		return nil
	}
	var ok bool
	for i := 0; i < 100; i++ {
		log, err := r.GetLog(r.CommitIndex)
		if err != nil {
			return err
		}

		if log.Term == r.CurrentTerm {
			ok = true
			break
		}

		time.Sleep(time.Second)
	}

	if !ok {
		return errors.New("client query: Timeout 1")
	}

	realIndex := r.CommitIndex

	r.BroadcastAppendEntries() // heartbeat only

	ok = false
	for i := 0; i < 100; i++ {
		if r.CommitIndex >= realIndex {
			ok = true
			break
		}

		time.Sleep(time.Second)
	}

	if !ok {
		return errors.New("client query: Timeout 2")
	}

	res, err := r.StateMachine.Process(0, 0, input.Query, 0)
	if err != nil {
		return err
	}

	*output = common.ClientQueryOutput{
		Status:     common.StatusOK,
		Response:   res,
		LeaderHint: "",
	}

	return nil
}
